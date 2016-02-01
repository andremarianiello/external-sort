{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}

module WordVector where

import qualified Data.Vector.Generic.Mutable as VGM
import qualified Data.Vector.Generic as VG

import qualified Data.Vector.Unboxed.Mutable as VUM
import qualified Data.Vector.Unboxed as VU

import Control.Applicative
import Data.Word
import Data.DoubleWord

newtype instance VUM.MVector s Word128 = MV_Word128 (VUM.MVector s Word64)
newtype instance VU.Vector Word128 = V_Word128 (VU.Vector Word64)

double = (* 2)
half = (`div` 2)

instance VGM.MVector VUM.MVector Word128 where
    basicLength (MV_Word128 v) = half $ VGM.basicLength v
    basicUnsafeSlice i len (MV_Word128 v) = MV_Word128 $ VGM.basicUnsafeSlice (double i) (double len) v
    basicOverlaps (MV_Word128 v1) (MV_Word128 v2) = VGM.basicOverlaps v1 v2
    basicUnsafeNew l = MV_Word128 <$> VGM.basicUnsafeNew (double l)
    basicInitialize (MV_Word128 v) = VGM.basicInitialize v
    basicUnsafeRead (MV_Word128 v) i = 
        Word128 <$> VGM.basicUnsafeRead v (double i) <*> VGM.basicUnsafeRead v (double i + 1)
    basicUnsafeWrite (MV_Word128 v) i (Word128 w1 w2) = 
        VGM.basicUnsafeWrite v (double i) w1 *> VGM.basicUnsafeWrite v (double i + 1) w2

instance VG.Vector VU.Vector Word128 where
    basicUnsafeFreeze (MV_Word128 v) = V_Word128 <$> VG.basicUnsafeFreeze v
    basicUnsafeThaw (V_Word128 v) = MV_Word128 <$> VG.basicUnsafeThaw v
    basicLength (V_Word128 v) = half $ VG.basicLength v
    basicUnsafeSlice i len (V_Word128 v) = V_Word128 $ VG.basicUnsafeSlice (double i) (double len) v
    basicUnsafeIndexM (V_Word128 v) i = Word128 <$> VG.basicUnsafeIndexM v (double i) <*> VG.basicUnsafeIndexM v (double i + 1)

instance VU.Unbox Word128
