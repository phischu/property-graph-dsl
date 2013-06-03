{-# LANGUAGE StandaloneDeriving, DeriveFunctor #-}
module Database.PropertyGraph (
	PropertyGraph,PropertyGraphT,
	newVertex,newEdge,
	Properties,Key,Value,
	VertexId,Label) where

import Database.PropertyGraph.Internal 