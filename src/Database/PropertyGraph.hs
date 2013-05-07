{-# LANGUAGE StandaloneDeriving, DeriveFunctor #-}
module Database.PropertyGraph where

import Control.Monad.Free (Free,liftF)

import Data.Text (Text)
import Data.Map (Map)

type Properties = Map Key Value
type Key = Text
type Value = Text
type VertexId = Integer
type EdgeId = Integer
type Label = Text

data PropertyGraphF a =
	NewVertex Properties (VertexId -> a) |
	NewEdge Properties Label VertexId VertexId (EdgeId -> a)

deriving instance Functor PropertyGraphF

type PropertyGraph = Free PropertyGraphF

type PG = PropertyGraph


newVertex :: Properties -> PG VertexId
newVertex properties = liftF (NewVertex properties id)

newEdge :: Properties -> Label -> VertexId -> VertexId -> PG EdgeId
newEdge properties label from to = liftF (NewEdge properties label from to id)



