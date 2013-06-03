module Database.PropertyGraph.Neo4jSingle (
    runPropertyGraphT) where

import Database.PropertyGraph.Internal (
    PropertyGraphT,PropertyGraphF(NewVertex))

import Database.Neo4j (Client,createNode)

import Control.Monad.Trans.Free (FreeF(Pure,Free),runFreeT)
import Control.Error (EitherT)

import Data.Map (toList)

import Control.Monad.IO.Class (MonadIO,liftIO)

runPropertyGraphT :: (MonadIO m) => PropertyGraphT m a -> Client -> EitherT Neo4jSingleError m a
runPropertyGraphT propertygraph client = do

    next <- runFreeT propertygraph

    case next of

        Pure x -> return x

        Free (NewVertex properties continue) -> do
            liftIO (createNode client (toList properties))


    return undefined

data Neo4jSingleError = Neo4jSingleError

