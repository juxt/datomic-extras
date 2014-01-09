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

(ns juxt.datomic.extras.spider)

(defn spider
  [row mapping]
  (letfn [(as-coll [c] (if (coll? c) c (list c)))
          (tr [f]
            (cond
             (vector? f) (fn [x] (filter (apply comp (reverse (map tr f))) x))
             ;; Collections treated like node-sets are in xpath
             (list? f) (partial map (first f))
             :otherwise f)
            )]
    (reduce-kv
     (fn [s k path]
       (if-let
           [val (reduce
                 (fn [e f] (when e ((tr f) e)))
                 row (as-coll path))]
         (assoc s k val)
         s))
     {} mapping)))
