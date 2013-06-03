module Database.PropertyGraph.Neo4jChunked where

import Database.PropertyGraph.Internal (
    PropertyGraphT,PropertyGraphF(NewVertex,NewEdge),
    VertexId(VertexId,unVertexId))
import Database.PropertyGraph.Neo4jBatch (add,vertexRequest,edgeRequest)

import Network.Http.Client (Hostname,Port)

import Control.Proxy (Proxy,Producer,Consumer,liftP,request,respond)
import Control.Proxy.Trans.State (StateP,modify,get)

import Control.Monad (forever)
import Control.Monad.IO.Class (MonadIO,liftIO)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Free (FreeF(Pure,Free),runFreeT)

import Data.Map (Map)
import qualified Data.Map as Map (lookup,union,empty)
import Data.Text (pack)

import qualified Data.Aeson as JSON (Value,json,toJSON)

-- | Interpret a property graph as the list of neo4j requests and a running unique Id.
interpretPropertyGraphT :: (Proxy p,Monad m) => PropertyGraphT m r -> () ->
    Producer (StateP Integer (StateP (Map Integer Integer) p)) JSON.Value m r
interpretPropertyGraphT propertygraph () = do

    temporaryid <- modify (+1) >> get

    next <- lift (runFreeT propertygraph)

    case next of

        Pure x -> return x

        Free (NewVertex properties continue) -> do
            respond (vertexRequest properties temporaryid)
            interpretPropertyGraphT (continue (VertexId temporaryid)) ()

        Free (NewEdge properties label fromid toid continue) -> do

            vertexidmap <- liftP get
            let temporaryOrExisting tempid = case Map.lookup tempid vertexidmap of
                    Nothing         -> pack ("{"++show tempid++"}")
                    Just existingid -> pack ("/node/"++show existingid)
                fromuri = temporaryOrExisting (unVertexId fromid)
                touri   = temporaryOrExisting (unVertexId toid)

            respond (edgeRequest properties label fromuri touri temporaryid)
            interpretPropertyGraphT continue ()

-- | Insert chunks of batch requests into a neo4j database...
insertPropertyGraphT :: (Proxy p,MonadIO m) => Hostname -> Port -> () -> Consumer (StateP (Map Integer Integer) p) [JSON.Value] m r
insertPropertyGraphT hostname port () = forever (do
    requestvalues <- request ()
    result <- liftIO (add hostname port (JSON.toJSON requestvalues))
    case result of
        Left e -> liftIO (print e)
        Right resultvalue -> do
            let vertexidmap = undefined
            modify (Map.union vertexidmap))




