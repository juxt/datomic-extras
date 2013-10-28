;; Copyright © 2013, JUXT LTD. All Rights Reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;;
;; By using this software in any fashion, you are agreeing to be bound by the
;; terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns datomic-extras.core
  (:refer-clojure :exclude (read-string))
  (:require
   [datomic.api :as d]
   [clojure.edn :as edn]
   [clojure.java.io :refer (resource)]
   [clojure.tools.logging :refer :all]))

(defn read-string [s]
  ;; We can't guarantee that Datomic was in the classpath when Clojure
  ;; was loaded, so these bindings may not have been picked up.
  (edn/read-string
   {:readers {'db/id datomic.db/id-literal
              'db/fn datomic.function/construct
              'base64 datomic.codec/base-64-literal}}
   s))

(defn db?
  "Check type is a Datomic database value. Useful for pre and post conditions."
  [db]
  (instance? datomic.db.Db db))

(defprotocol DatomicConnection
  (as-conn [_]))

(extend-protocol DatomicConnection
  datomic.Connection
  (as-conn [c] c)
  java.lang.String
  (as-conn [dburi] (d/connect dburi)))

(defprotocol DatabaseReference
  (as-db [_]))

(extend-protocol DatabaseReference
  datomic.db.Db
  (as-db [db] db)
  datomic.Connection
  (as-db [conn] (d/db conn))
  java.lang.String
  (as-db [dburi] (as-db (d/connect dburi))))

(defprotocol EntityReference
  (to-ref-id [_])
  (to-entity-map [_ db]))

(extend-protocol EntityReference
  datomic.query.EntityMap
  (to-ref-id [em] (:db/id em))
  (to-entity-map [em _] em)
  java.lang.Long
  (to-ref-id [id] id)
  (to-entity-map [id db] (d/entity (as-db db) id))
  clojure.lang.Keyword
  (to-ref-id [k] k)
  (to-entity-map [k db] (d/entity (as-db db) k))
  java.lang.String
  (to-ref-id [id] (to-ref-id (Long/parseLong id)))
  (to-entity-map [id db] (to-entity-map (Long/parseLong id) db))
  clojure.lang.PersistentVector
  (to-entity-map [id db] (to-entity-map (first id) db))
  )

(defn init [dburi schema-resource]
  (if (d/create-database dburi)
    (debug "Created database" dburi)
    (debug "Using existing database" dburi))
  (let [conn (d/connect dburi)]
    (debug "Loading schema")
    @(d/transact conn (read-string (slurp (resource schema-resource))))
    conn))

(defn transact-async
  "A wrapper over Datomic's transact-async, allowing client to pass a
  dburi as well as a connection."
  [conn txdata]
  (d/transact-async (as-conn conn) txdata))

(defn insert
  "Asynchronously insert. A @/deref will return the entity inserted. The
   txdata parameter should be a function with one argument, a temporary
   entity id, which will be created if necessary."
  ([conn txdata e]
      (let [p (promise)]
        (let [{:keys [db-after tempids]}
              @(transact-async conn (txdata e))]
          (deliver p (to-entity-map (d/resolve-tempid db-after tempids e) db-after)))))
  ([conn txdata]
     (insert conn txdata (d/tempid :db.part/user))))
