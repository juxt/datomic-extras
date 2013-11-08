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

(require '(clojure [string :refer (join)]
                   [edn :as edn])
         '(clojure.java [shell :refer (sh)]
                        [io :as io]))

(def default-version "0.0.1-SNAPSHOT")

(defn head-ok []
  (-> (sh "git" "rev-parse" "--verify" "HEAD")
      :exit zero?))

(defn refresh-index []
  (sh "git" "update-index" "-q" "--ignore-submodules" "--refresh"))

(defn unstaged-changes []
  (-> (sh "git" "diff-files" "--quiet" "--ignore-submodules")
      :exit zero? not))

(defn uncommitted-changes []
  (-> (sh "git" "diff-index" "--cached" "--quiet" "--ignore-submodules" "HEAD" "--")
      :exit zero? not))

;; We don't want to keep having to 'bump' the version when we are
;; sitting on a more capable versioning system: git.
(defn get-version []
  (cond
   (not (let [gitdir (io/file ".git")]
          (and (.exists gitdir)
               (.isDirectory gitdir))))
   default-version

   (not (head-ok)) (throw (ex-info "HEAD not valid" {}))

   :otherwise
   (do
     (refresh-index)
     (let [{:keys [exit out err]} (sh "git" "describe" "--tags" "--long")]
       (if (= 128 exit) default-version
           (let [[[_ tag commits hash]] (re-seq #"(.*)-(.*)-(.*)" out)]
             (if (and
                  (zero? (edn/read-string commits))
                  (not (unstaged-changes))
                  (not (uncommitted-changes)))
               tag
               (let [[[_ stem lst]] (re-seq #"(.*\.)(.*)" tag)]
                 (join [stem (inc (read-string lst)) "-" "SNAPSHOT"])))))))))

(defproject juxt/datomic-extras (get-version)
  :description "Datomic utilities that have been found useful."
  :url "https://github.com/juxt/datomic-extras"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.datomic/datomic-free "0.8.4007"]])
