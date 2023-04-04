(ns fjson
  (:require [charred.api :as charred]
            [ham-fisted.api :as ham-fisted]
            [clojure.data.json :as clj-json]
            [jsonista.core :as jsonista]
            [criterium.core :as crit]
            [clojure.edn :as edn]
            [tech.viz.pyplot :as pyplot]
            [clojure.java.io :as io]
            [applied-science.darkstar :as darkstar]
            [clojure.pprint :as pp])
  (:import [com.fasterxml.jackson.databind ObjectMapper]
           [charred JSONReader$ObjReader JSONReader$ArrayReader]
           [ham_fisted MutHashTable]
           [java.util List Map ArrayDeque])
  (:gen-class))

(set! *warn-on-reflection* true)

(def testfiles (->> (-> (java.io.File. "data/")
                        (.list))
                    (filter #(.endsWith (str %) ".json"))
                    (map (fn [fname]
                           [fname (slurp (str "data/" fname))]))
                    (flatten)
                    (apply array-map)))


(defn jsonista-mutable
  []
  (let [mapper (doto (ObjectMapper.)
                 (.registerModule (jsonista/java-collection-module)))]
    #(jsonista/read-value % mapper)))

(definterface IEventParser
  (onEvents [event-data]))

(deftype EventParser [^ArrayDeque stack
                      ^{:unsynchronized-mutable true} tos]
  IEventParser
  (onEvents [this event-data]
    (set! tos
          (reduce (fn [tos evt]
                    (cond
                      (identical? :new-obj evt)
                      (do
                        (.add stack (MutHashTable. ham-fisted/equal-hash-provider))
                        tos)
                      (identical? :new-array evt)
                      (do (.add stack (ham-fisted/object-array-list))
                          tos)
                      (identical? :end-obj evt)
                      (persistent! (.pollLast stack))
                      (identical? :end-array evt)
                      (.pollLast stack)
                      (vector? evt)
                      (let [k (.get ^List evt 0)
                            v (.get ^List evt 1)
                            end-idx (unchecked-dec (.size stack))]
                        (.put ^Map (.peekLast stack) k (if (identical? v :tos)
                                                         tos
                                                         v))
                        tos)
                      :else
                      (do
                        (.add ^List (.peekLast stack) (if (identical? evt :tos)
                                                        tos
                                                        evt)))))
                  tos
                  event-data)))
  clojure.lang.IDeref
  (deref [this] tos))


(defn parse-events [events]
  (let [p (EventParser. (ArrayDeque.) nil)]
    (.onEvents p events)
    @p))


(def test-parse-fns
  {:charred-manual-keyword
   (charred/parse-json-fn {:ary-iface
                           (reify JSONReader$ArrayReader
                             (newArray [this] (ham-fisted/object-array-list))
                             (onValue [this m v] (.add ^List m v) m)
                             (finalizeArray [this m] (persistent! m)))
                           :obj-iface
                           (reify JSONReader$ObjReader
                             (newObj [this]
                               (ham-fisted/mut-map))
                             (onKV [this m k v]
                               (.put ^Map m (keyword k) v) m)
                             (finalizeObj [this m] (persistent! m)))})
   :charred-auto-keyword
   (charred/parse-json-fn {:key-fn keyword
                           :ary-iface
                           (reify JSONReader$ArrayReader
                             (newArray [this] (ham-fisted/object-array-list))
                             (onValue [this m v] (.add ^List m v) m)
                             (finalizeArray [this m] (persistent! m)))
                           :obj-iface
                           (reify JSONReader$ObjReader
                             (newObj [this]
                               (ham-fisted/mut-map))
                             (onKV [this m k v]
                               (.put ^Map m k v) m)
                             (finalizeObj [this m] (persistent! m)))})

   :charred-immut-auto
   (charred/parse-json-fn {:key-fn keyword})
   :charred-immut-manual
   (charred/parse-json-fn {:key-fn #(keyword %)})
   :jsonista-immut-keyword
   (let [mapper (jsonista/object-mapper {:decode-key-fn keyword})]
     #(jsonista/read-value % mapper))})



(def parse-fns
  {:jsonista-mutable (jsonista-mutable)
   :charred-mutable (charred/parse-json-fn {:profile :mutable})
   :charred-immutable (charred/parse-json-fn {:profile :immutable})
   ;;Also produces persistent datastructures
   :charred-hamf (charred/parse-json-fn {:ary-iface
                                         (reify JSONReader$ArrayReader
                                           (newArray [this] (ham-fisted/object-array-list))
                                           (onValue [this m v] (.add ^List m v) m)
                                           (finalizeArray [this m] (persistent! m)))
                                         :obj-iface
                                         (reify JSONReader$ObjReader
                                           (newObj [this]
                                             (MutHashTable. ham-fisted/equal-hash-provider))
                                           (onKV [this m k v]
                                             (.put ^Map m k v) m)
                                           (finalizeObj [this m] (persistent! m)))})
   ;;test pathway to produce a stream of events to see what the max speed of the parser really
   ;;is - turns out with latest charred this isn't much faster than mutable or hamf as the
   ;;map keys are canonicalized.
   ;; :charred-events
   ;; (charred/parse-json-fn
   ;;  {:parser-fn
   ;;   (fn []
   ;;     (let [data (ham-fisted/object-array-list)]
   ;;       {:array-iface
   ;;        (reify JSONReader$ArrayReader
   ;;          (newArray [this] (.add data :new-array) :new-array)
   ;;          (onValue [this m v]
   ;;            (.add data (if (keyword? v)
   ;;                         :tos
   ;;                         v))
   ;;                   m)
   ;;          (finalizeArray [this m] (.add data :end-array) :end-array))
   ;;        :obj-iface
   ;;        (reify JSONReader$ObjReader
   ;;          (newObj [this] (.add data :new-obj) :new-obj)
   ;;          (onKV [this m k v] (.add data [k (if (keyword? v)
   ;;                                             :tos
   ;;                                             v)]) m)
   ;;          (finalizeObj [this m] (.add data :end-obj) :end-obj))
   ;;        :finalize-fn (constantly data)}))})
})


(defmacro benchmark-ms
  [op]
  `(let [bdata# (crit/quick-benchmark ~op nil)]
     {:mean (* (double (first (:mean bdata#))) 1e3)
      :variance (* (double (first (:variance bdata#))) 1e3)}))


(defn benchmark-data
  []
  (->> testfiles
       (map (fn [[fname fdata]]
              {:name fname
               :length (count fdata)
               :results (->> parse-fns
                             (map (fn [[fn-name parse-fn]]
                                    (assoc (benchmark-ms (parse-fn fdata))
                                           :engine fn-name)))
                             (sort-by :mean))}))
       (sort-by :length)))


(defn benchmark->file
  [fname]
  (spit fname (with-out-str (pp/pprint  (benchmark-data)))))


(defn pp-file
  [fname]
  (spit fname (with-out-str (pp/pprint (edn/read-string (slurp fname))))))

(comment
  (let [jv (System/getProperty "java.version")
        fname (cond
                (.startsWith jv "17.0") "jdk-17.edn"
                (.startsWith jv "19.0") "jdk-19.edn"
                (.startsWith jv "1.8") "jdk-8.edn"
                :else (throw (Exception. "Unrecognized jvm version")))]
    (benchmark->file fname)
    (println "wrote" fname))

  )


(defn flatten-results
  [fnames]
  (->> fnames
       (mapcat (fn [fname]
                 (let [jdk-name (.substring (str fname) 0 (- (count fname) 4))
                       raw-data (with-open [is (io/reader fname)]
                                  (edn/read (java.io.PushbackReader. is)))]
                   (->> raw-data
                        (mapcat (fn [file-results]
                                  (->> (:results file-results)
                                       (map #(assoc % :jdk jdk-name
                                                    :name (:name file-results)
                                                    :length (:length file-results))))))))))))

(defn chart-results
  ([& [fnames]]
   (let [fnames (or fnames (filter #(and (.startsWith ^String % "jdk-")
                                         (.endsWith ^String % "edn"))
                                   (.list (java.io.File. "./"))))]
     (spit "docs/chart.svg"
           (-> {:$schema "https://vega.github.io/schema/vega-lite/v5.1.0.json"
                :mark {:type :point}
                :width 800
                :height 600
                :data {:values (vec (flatten-results fnames))}
                :encoding
                {:y {:field :mean, :type :quantitative :axis {:grid false}}
                 :x {:field :length :type :quantitative}
                 :color {:field :jdk :type :nominal}
                 :shape {:field :engine :type :nominal}}}
               (charred/write-json-str)
               (darkstar/vega-lite-spec->svg))))))


(comment
  (chart-results)
  )


(defn test-keyword-deserialize
  []
  (println "jsonista-immut-keyword")
  (crit/quick-bench ((test-parse-fns :jsonista-immut-keyword) (testfiles "json100k.json")))
  (println "charred-immut-keyword")
  (crit/quick-bench ((test-parse-fns :charred-immut-auto) (testfiles "json100k.json")))
  (println "charred-hamf-keyword")
  (crit/quick-bench ((test-parse-fns :charred-auto-keyword) (testfiles "json100k.json")))
  )


(defn -main
  [& args]
  #_(if (== 0 (count args))
    (let [jv (System/getProperty "java.version")
          fname (cond
                  (.startsWith jv "17.0") "jdk-17.edn"
                  (.startsWith jv "19.0") "jdk-19.edn"
                  (.startsWith jv "1.8") "jdk-8.edn"
                  :else (throw (Exception. "Unrecognized jvm version")))]
      (benchmark->file fname)
      (println "wrote" fname))
    (chart-results))
  (test-keyword-deserialize)
  (println "done"))


(comment
  (def bigtext (slurp "data/json100k.json"))
  (def job (with-open [jfn (char-input/read-json-fn bigtext)]
             (jfn)))
  (require '[tech.v3.dataset :as ds])
  (-> (ds/->dataset "../../tech.all/tech.ml.dataset/test/data/stocks.csv")
      (ds/column-map "date" str ["date"])
      (ds/write! "data/stocks.json"))

  (benchmark-ms (char-input/read-json bigtext))
  ;; 560us
  (benchmark-ms (char-input/read-json bigtext :profile :mutable))
  ;; 345us
  (benchmark-ms (char-input/read-json bigtext :profile :raw))
  ;; 272us


  (dotimes [idx 10000]
    (let [jfn (char-input/read-json-fn bigtext :profile :mutable)]
      (jfn)))

  (let [data (second (nth testfiles 2))
        jfn (char-input/parse-json-fn {:profile :mutable})]
    (dotimes [idx 100000000]
      (jfn data))
    )

  (def jdata (char-input/read-json (java.io.File. "data/json100k.json")))


  (dotimes [idx 100000]
    (clj-json/write-str jdata))
  )
