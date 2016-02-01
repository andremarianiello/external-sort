{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

module Word128 where

import Data.Coerce
import Data.Bits
import Data.Word
import Data.Vector.Unboxed.Deriving

newtype Word128 = W128 (Word64, Word64) deriving (Eq, Ord, Show)
derivingUnbox "Word128" 
    [t| () => Word128 -> (Word64, Word64) |]
    [|coerce|]
    [|coerce|]

instance Num Word128 where
    W128 (ah, al) + W128 (bh, bl) =
        let rl = al + bl
            rh = ah + bh + if rl < al then 1 else 0
        in W128 (rh, rl)
    W128 (ah, al) - W128 (bh, bl) =
        let rl = al - bl
            rh = ah - bh - if rl > al then 1 else 0
        in W128 (rh, rl)
    a * b = go 0 0 where
        go 128 r = r
        go i   r
            | testBit b i  = go (i+1) (r + (a `shiftL` i))
            | otherwise    = go (i+1) r
    negate a = 0 - a
    abs a = a
    signum a = if a > 0 then 1 else 0
    fromInteger i = W128 (fromIntegral $ i `shiftR` 64, fromIntegral i)

pointwise op (W128 (a, b)) = W128 (op a, op b)
pointwise2 op (W128 (a, b)) (W128 (c, d)) = W128 (op a c, op b d)

instance FiniteBits Word128 where
    finiteBitSize ~(W128 (a, b)) = finiteBitSize a + finiteBitSize b

instance Bits Word128 where
    popCount (W128 (h, l)) = popCount h + popCount l
    bit i | i >= 64    = W128 (bit $ i - 64, 0)
        | otherwise = W128 (0, bit i)
    complement = pointwise complement
    (.&.) = pointwise2 (.&.)
    (.|.) = pointwise2 (.|.)
    xor = pointwise2 xor
    setBit (W128 (h, l)) i
        | i >= 64   = W128 (setBit h (i - 64), l)
        | otherwise = W128 (h, setBit l i)
    shiftL (W128 (h, l)) i
        | i > finiteBitSize l = shiftL (W128 (l, 0)) (i - finiteBitSize l)
        | otherwise     = W128 ((h `shiftL` i) .|. (l `shiftR` (finiteBitSize l - i)), l `shiftL` i)
    shiftR (W128 (h, l)) i 
        | i > finiteBitSize h = shiftR (W128 (0, h)) (i - finiteBitSize h)
        | otherwise     = W128 (h `shiftR` i, (l `shiftR` i) .|. h `shiftL` (finiteBitSize h - i))
    isSigned _ = False
    testBit (W128 (h, l)) i
        | i >= finiteBitSize l = testBit h (i - finiteBitSize l)
        | otherwise      = testBit l i
    rotateL w i = shiftL w i .|. shiftR w (128 - i)
    rotateR w i = shiftR w i .|. shiftL w (128 - i)
    bitSize _ = 128
    bitSizeMaybe _ = Just 128

instance Enum Word128 where
    toEnum i            = W128 (0, (toEnum i))
    fromEnum (W128 (_, l)) = fromEnum l
    pred (W128 (h, 0)) = W128 (pred h, maxBound)
    pred (W128 (h, l)) = W128 (h, pred l)
    succ (W128 (h, l)) = if l == maxBound then W128 (succ h, 0) else W128 (h, succ l)


instance Real Word128 where
    toRational w = toRational (fromIntegral w :: Integer)

instance Integral Word128 where
    toInteger (W128 (h, l)) = (fromIntegral h `shiftL` finiteBitSize l) + fromIntegral l
    divMod = quotRem
    quotRem a@(W128 (ah, al)) b@(W128 (bh, bl)) =
        let r = a - q*b
            q = go 0 (finiteBitSize a) 0
        in (q,r)
        where
            -- Trivial long division
            go :: Word128 -> Int -> Word128 -> Word128
            go t 0 v = if v >=  b then t+1 else t
            go t i v
                | v >= b    = go (setBit t i) i' v2
                | otherwise = go t i' v1
                    where
                    i' = i - 1
                    newBit = if (testBit a i') then 1 else 0
                    v1 = (v `shiftL` 1) .|. newBit
                    v2 = ((v - b) `shiftL` 1) .|. newBit
