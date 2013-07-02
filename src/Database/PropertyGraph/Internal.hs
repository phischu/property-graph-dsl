{-# LANGUAGE StandaloneDeriving, DeriveFunctor, GeneralizedNewtypeDeriving #-}
module Database.PropertyGraph.Internal where

import Control.Monad.Trans.Free (FreeT,liftF)

import Control.Monad.Identity (Identity)

import Data.Text (Text)
import Data.Map (Map)

import qualified Data.Aeson as JSON (Value)

-- | A monadic property graph dsl. A property graph consists of
--   vertices and edges each with properties. Edges also must have
--   labels.
type PropertyGraph = PropertyGraphT Identity

-- | The property graph monad transformer. Construction of the
--   property graph is interleaved with other effects.
type PropertyGraphT = FreeT PropertyGraphF

data PropertyGraphF a =
	NewVertex Properties [Label] (VertexId -> a) |
	NewEdge Properties Label VertexId VertexId a

-- | The properties of either a vertex or an edge. Represented as
--   a 'Map' from property names to property values.
type Properties = Map Key Value

-- | Propertynames must all be textual.
type Key = Text

-- | Currently property values must be textual as well.
type Value = JSON.Value

-- | A unique identifier for vertices. Internally an Integer but
--   kept abstract to prevent disaster.
newtype VertexId = VertexId Integer

-- | Each edge is required to have a textual label.
type Label = Text

deriving instance Functor PropertyGraphF

-- | Within the property graph monad create a new vertex with the
--   given properties. The resulting 'VertexId' can be bound and used
--   subsequently.
newVertex :: (Monad m) => Properties -> [Label] -> PropertyGraphT m VertexId
newVertex properties labels = liftF (NewVertex properties labels id)

-- | Withing the property graph monad create a new edge with the given
--   properties and label. It goes from the vertex with the first given
--   'VertexId' to the one with the second given 'VertexId'.
newEdge :: (Monad m) => Properties -> Label -> VertexId -> VertexId -> PropertyGraphT m ()
newEdge properties label from to = liftF (NewEdge properties label from to ())



