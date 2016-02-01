{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

module Main where

import Control.Monad.Trans.Resource
import Control.Monad.IO.Class
import Control.Monad.Trans.Class
import Data.Conduit
import Data.Conduit.Internal (ConduitM(..), Pipe(..))
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit.Serialization.Binary as CS
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Combinators as CC
import qualified Data.ByteString.Char8 as BC
import Data.ByteString.Lex.Integral
import qualified Data.ByteString.Builder as BB

import Control.Monad
import Control.Monad.Catch
import Control.Monad.ST
import Data.Bits
import Data.Binary
import Data.Binary.Get (getWord64be)
import Data.DoubleWord
import Data.Maybe
import Data.Monoid
import Data.List ((\\))
import WordVector
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Generic.Mutable as VGM
import qualified Data.Vector.Unboxed.Mutable as VUM
import qualified Data.Vector.Algorithms.Intro as V

import System.Directory
import System.IO


instance Exception [Char]

type IPChunk = VU.Vector Word128

inFile = "/home/andre/Documents/external-sort/challenge-data-set"
outFile = "/home/andre/Documents/external-sort/out"
workDir = "/home/andre/Documents/external-sort/work/"

chunkSize = 10000000
updateInterval = div chunkSize 10

main :: IO ()
main = do
    removeDirectoryRecursive workDir
    createDirectory workDir

    putStrLn "Chunking and sorting"
    let chunkAndSort = CB.lines =$= clean =$= removeColons =$= hexToWord128 =$= countC "Hex in: " =$= CC.conduitVector chunkSize =$= chunkSort
    runResourceT $ CB.sourceFile inFile $$ chunkAndSort =$= saveChunk workDir

    chunkSources <- map (=$= bytesToWord128) <$> sourceWholeDir workDir

    putStrLn "Merging"
    runResourceT $ merged chunkSources $$ word128ToHex =$= withNewline =$= countC "Hex out: "=$= sinkFileBuilder (Right outFile)

countC :: MonadIO m => String -> Conduit a m a
countC str = go 1 where
    go n = do
        ma <- await
        case ma of
            Nothing -> return ()
            Just a -> do
                when ((n `mod` updateInterval) == 0) $ liftIO $ putStrLn $ str ++ show n
                yield a
                go (n+1)

-- stage one pieces

clean :: Monad m => Conduit BC.ByteString m BC.ByteString
clean = CL.filter (not . BC.null)

removeColons :: Monad m => Conduit BC.ByteString m BC.ByteString
removeColons = CL.map $ BC.filter (/= ':')

hexToWord128 :: MonadThrow m => Conduit BC.ByteString m Word128
hexToWord128 = CL.mapM $ \bs -> do
    case readHexadecimal bs of
        Nothing -> throwM $ "Failed to parse hex prefix: " ++ BC.unpack bs
        Just (hexNum, rest) -> if BC.length rest == 0
            then return hexNum
            else throwM $ "Malformed IP: " ++ BC.unpack bs

chunkSort :: Monad m => Conduit IPChunk m IPChunk
chunkSort = CL.map $ \v -> VU.create $ do
        mv <- VU.thaw v
        V.sort mv
        return mv

word128ToBytes :: Monad m => Conduit Word128 m BB.Builder
word128ToBytes = CL.map $ \(Word128 w1 w2) -> BB.word64BE w1 <> BB.word64BE w2

saveChunk :: MonadResource m => FilePath -> Sink IPChunk m ()
saveChunk dir = do
    awaitForever $ \chunk -> liftIO $ runResourceT $ do
        CC.yieldMany chunk $$ word128ToBytes =$= countC "Mid write: " =$= sinkFileBuilder (Left dir) 

-- stage two pieces

sourceWholeDir :: (MonadResource mr, MonadIO m) => FilePath -> m [Source mr BC.ByteString]
sourceWholeDir filepath = do
    chunkPaths <- liftIO $ getDirectoryContents filepath
    return $ map (CB.sourceFile . (workDir ++)) (chunkPaths \\ [".", ".."])

bytesToWord128 :: MonadThrow m => Conduit BC.ByteString m Word128
bytesToWord128 = CS.conduitGet $ Word128 <$> getWord64be <*> getWord64be

merged :: (Ord a, Monad m) => [Source m a] -> Source m a
merged [] = return ()
merged [s] = s
merged ss = mergeTwo (merged s1) (merged s2) where
    (s1, s2) = halve ss
    halve xs = splitAt (length xs `div` 2) xs

word128ToHex :: (Monad m) => Conduit Word128 m BB.Builder
word128ToHex = CL.map $ \(Word128 w1 w2) -> BB.word64HexFixed w1 <> BB.word64HexFixed w2

withNewline :: (Monad m) => Conduit BB.Builder m BB.Builder
withNewline = CL.map (<> BB.byteString (BC.singleton '\n'))

-- misc

sinkFileBuilder :: (MonadResource m) => Either FilePath FilePath -> Sink BB.Builder m ()
sinkFileBuilder filepath = bracketP acquire finalize use where
    acquire = case filepath of
        Left tempDir -> snd <$> openTempFile tempDir ""
        Right fileName -> openFile fileName WriteMode
    finalize = hClose
    use handle = awaitForever $ liftIO . BB.hPutBuilder handle

mergeTwo :: (Ord a, Monad m) => Source m a -> Source m a -> Source m a
mergeTwo (ConduitM s1) (ConduitM s2) = ConduitM $ \k -> mergePipes (s1 k) (s2 k) where
    -- if either pipe finishes, use the other one
    mergePipes (Done _) p = p
    mergePipes p (Done _) = p

    -- run both pipes
    mergePipes (PipeM m1) (PipeM m2) = PipeM $ mergePipes <$> m1 <*> m2

    -- if one pipe has output ready, run the other one
    mergePipes (PipeM m) p@HaveOutput{} = PipeM $ mergePipes p <$> m
    mergePipes p@HaveOutput{} (PipeM m) = PipeM $ mergePipes p <$> m

    -- if both pipe have output, do the actual merging
    mergePipes p1@(HaveOutput next1 finalizer1 o1) p2@(HaveOutput next2 finalizer2 o2)
        | o1 < o2   = HaveOutput (mergePipes next1 p2) finalizer1 o1
        | otherwise = HaveOutput (mergePipes p1 next2) finalizer2 o2

    -- if either pipe asks for input, provide ()
    mergePipes (NeedInput _ k) p = mergePipes (k ()) p
    mergePipes p (NeedInput _ k) = mergePipes p (k ())

    -- if either pipe has leftovers they aren't needed, so continue
    mergePipes (Leftover left ()) right = mergePipes left right
    mergePipes left (Leftover right ()) = mergePipes left right
