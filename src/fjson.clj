(ns fjson
  (:require [tech.v3.datatype.char-input :as char-input]
            [clojure.data.json :as clj-json]
            [jsonista.core :as jsonista]
            [criterium.core :as crit]
            [clojure.edn :as edn]
            [tech.viz.pyplot :as pyplot]
            [clojure.java.io :as io]
            [clojure.pprint :as pp])
  (:import [com.fasterxml.jackson.databind ObjectMapper]))

(def testfiles (->> (-> (java.io.File. "data/")
                        (.list))
                    (filter #(.endsWith (str %) ".json"))
                    (mapv (fn [fname]
                            [fname (slurp (str "data/" fname))]))))


(defn jsonista-mutable
  []
  (let [mapper (doto (ObjectMapper.)
                 (.registerModule (jsonista/java-collection-module)))]
    #(jsonista/read-value % mapper)))


(def parse-fns
  {:clj-json #(clj-json/read-str %)
   :jsonista-immutable #(jsonista/read-value %)
   :jsonista-mutable (jsonista-mutable)
   :dtype-immutable (char-input/parse-json-fn {:profile :immutable})
   :dtype-mutable (char-input/parse-json-fn {:profile :mutable})})


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
                (.startsWith jv "1.8") "jdk-8.edn"
                :else (throw (Exception. "Unrecognized jvm version")))]
    (benchmark->file fname))

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
   (-> {:$schema "https://vega.github.io/schema/vega-lite/v5.1.0.json"
        :mark {:type :point}
        :width 800
        :height 600
        :data {:values (vec (flatten-results (or fnames ["jdk-8.edn" "jdk-17.edn"])))}
        :encoding
        {:y {:field :mean, :type :quantitative :axis {:grid false}}
         :x {:field :length :type :quantitative}
         :color {:field :jdk :type :nominal}
         :shape {:field :engine :type :nominal}}}
       (pyplot/show))))


(comment
  (chart-results)
  )


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
  )
