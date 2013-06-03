module Database.PropertyGraph.Neo4jSingle (
    runPropertyGraphT) where

import Database.PropertyGraph.Internal (
    PropertyGraphT,PropertyGraphF(NewVertex,NewEdge),
    VertexId(VertexId))

import Database.Neo4j (
    Client,createNode,createRelationship,
    lookupNode,getNodeID)

import Control.Monad.Trans.Free (FreeF(Pure,Free),runFreeT)
import Control.Error (EitherT,left)

import Data.Map (toList)
import Data.Text (unpack)

import Control.Monad.Trans (lift)
import Control.Monad.IO.Class (MonadIO,liftIO)

runPropertyGraphT :: (MonadIO m) => PropertyGraphT m a -> Client -> EitherT Neo4jSingleError m a
runPropertyGraphT propertygraph client = do

    next <- lift (runFreeT propertygraph)

    case next of

        Pure x -> return x

        Free (NewVertex properties continue) -> do
            node <- liftIO (createNode client (toList properties)) >>= either (left . NodeCreationError) return
            runPropertyGraphT (continue (VertexId (getNodeID node))) client

        Free (NewEdge properties label (VertexId from) (VertexId to) continue) -> do
            fromNode <- liftIO (lookupNode client from) >>= either (left . NodeLookupError) return
            toNode   <- liftIO (lookupNode client to)   >>= either (left . NodeLookupError) return
            liftIO (createRelationship client fromNode toNode (unpack label) (toList properties))
            runPropertyGraphT continue client


    return undefined

data Neo4jSingleError = NodeCreationError String
                      | NodeLookupError String

