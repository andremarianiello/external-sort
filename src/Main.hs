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
import Data.DoubleWord
import Data.Maybe
import Data.Monoid
import Data.List ((\\))
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Generic.Mutable as VGM
import qualified Data.Vector.Unboxed.Mutable as VUM
import qualified Data.Vector.Algorithms.AmericanFlag as VA

import System.Directory
import System.Environment
import System.IO


instance Exception [Char]

type IPChunk = VU.Vector Word128

chunkSize = 12500000
updateInterval = div chunkSize 10

main :: IO ()
main = do
    --handle args
    args <- getArgs
    wd <- getCurrentDirectory
    (inFile, outFile) <- case args of
        [a1, a2] -> return (a1, a2)
        _ -> error $ "usage: external-sort <inFile> <outFile>"

    --set up workspace for chunk files
    tmpDir <- getTemporaryDirectory
    let workDir = tmpDir ++ "/work/"
    exists <- doesDirectoryExist workDir
    when exists $ removeDirectoryRecursive workDir
    createDirectory workDir
        
    -- process
    putStrLn "Chunking and sorting"
    let chunkAndSort = CB.lines =$= countC "Hex in: " =$= CC.conduitVector chunkSize =$= chunkSort
    runResourceT $ CB.sourceFile inFile $$ chunkAndSort =$= saveChunk workDir

    chunkSources <- sourceWholeDir workDir

    putStrLn "Merging"
    runResourceT $ merged chunkSources $$ countC "Hex out: " =$= CC.unlinesAscii =$= CB.sinkFile outFile

-- debugging

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

chunkSort :: Monad m => Conduit (V.Vector BC.ByteString) m (V.Vector BC.ByteString)
chunkSort = CL.map $ \v -> runST $ do
        mv <- V.unsafeThaw v
        VA.sort mv
        V.unsafeFreeze mv

saveChunk :: MonadResource m => FilePath -> Sink (V.Vector BC.ByteString) m ()
saveChunk dir = do
    awaitForever $ \chunk -> liftIO $ runResourceT $ do
        CC.yieldMany chunk $$ countC "Mid write: " =$= CC.unlinesAscii =$= sinkTempFileDir
    where
        sinkTempFileDir = bracketP open hClose CB.sinkHandle
        open = snd <$> openTempFile dir "chunk"

-- stage two pieces

sourceWholeDir :: (MonadResource mr, MonadIO m) => FilePath -> m [Source mr BC.ByteString]
sourceWholeDir filepath = do
    chunkPaths <- liftIO $ getDirectoryContents filepath
    return $ map (\path -> CB.sourceFile (filepath ++ path) =$= CB.lines) (chunkPaths \\ [".", ".."])

merged :: (Ord a, Monad m) => [Source m a] -> Source m a
merged [] = return ()
merged [s] = s
merged ss = mergeTwo (merged s1) (merged s2) where
    (s1, s2) = halve ss
    halve xs = splitAt (length xs `div` 2) xs

-- misc

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
