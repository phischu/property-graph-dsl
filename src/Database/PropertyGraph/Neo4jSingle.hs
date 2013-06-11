{-# LANGUAGE StandaloneDeriving #-}

module Database.PropertyGraph.Neo4jSingle (
    runPropertyGraphT,
    interpretPropertyGraphT,
    Neo4jSingleError(..)) where

import Database.PropertyGraph.Internal (
    PropertyGraphT,PropertyGraphF(NewVertex,NewEdge),
    VertexId(VertexId))

import Database.Neo4j (
    Client,createNode,createRelationship,
    lookupNode,getNodeID)

import Control.Monad.Trans.Free (FreeF(Pure,Free),runFreeT)
import Control.Error (runEitherT,EitherT,left)

import Data.Map (toList)
import Data.Text (unpack)

import Control.Monad.Trans (lift)
import Control.Monad.IO.Class (MonadIO,liftIO)

-- | Insert a property graph into a neo4j database returning errors as 'Left's.
runPropertyGraphT :: (MonadIO m) => Client -> PropertyGraphT m a -> m (Either Neo4jSingleError a)
runPropertyGraphT client propertygraph = runEitherT (interpretPropertyGraphT client propertygraph)

-- | Insert a property graph into a neo4j database return errors in the 'EitherT' monad.
interpretPropertyGraphT :: (MonadIO m) => Client -> PropertyGraphT m a -> EitherT Neo4jSingleError m a
interpretPropertyGraphT client propertygraph = do

    next <- lift (runFreeT propertygraph)

    case next of

        Pure x -> return x

        Free (NewVertex properties continue) -> do
            node <- liftIO (createNode client (toList properties)) >>= either (left . NodeCreationError) return
            interpretPropertyGraphT client (continue (VertexId (getNodeID node)))

        Free (NewEdge properties label (VertexId from) (VertexId to) continue) -> do
            fromNode <- liftIO (lookupNode client from)
                >>= either (left . NodeLookupError) return
            toNode   <- liftIO (lookupNode client to)
                >>= either (left . NodeLookupError) return
            _ <- liftIO (createRelationship client fromNode toNode (unpack label) (toList properties))
                >>= either (left . RelationshipCreationError) return
            interpretPropertyGraphT client continue

-- | The different kinds of errors that may occure during insertion of a property
--   graph into a neo4j database.
data Neo4jSingleError = NodeCreationError String
                      | RelationshipCreationError String
                      | NodeLookupError String

deriving instance Show Neo4jSingleError
