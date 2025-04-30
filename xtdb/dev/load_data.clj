(ns load-data
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [next.jdbc :as jdbc]
            [xtdb.next.jdbc :as xt-jdbc]
            [clojure.tools.logging :as log]))

(def db-spec {:dbtype "xtdb"
              :dbname "xtdb"
              :host "localhost"
              :user "your-username"
              :password "your-password"})

(defn read-tsv-files [dir]
  (->> (file-seq (io/file dir))
       (filter #(.isFile %))
       (filter #(str/ends-with? (.getName %) ".tsv"))
       (reduce (fn [acc file]
                 (let [table-name (-> (.getName file)
                                      (str/replace #"\.tsv$" "")
                                      (str/replace #"^public_" ""))]
                   (assoc acc table-name (.getAbsolutePath file))))
               {})))

(defn parse-value [value]
  (cond
    (re-matches #"-?\d+(\.\d+)?" value)
    (if (re-find #"\." value)
      (Double/parseDouble value)
      (Long/parseLong value))

    (re-matches #"(?i)(t|true|f|false)" value)
    (boolean (or (= "t" (str/lower-case value))
                 (= "true" (str/lower-case value))))

    ;; Exact ISO date (yyyy-MM-dd)
    (re-matches #"\d{4}-\d{2}-\d{2}" value)
    (try
      (java.time.LocalDate/parse value)
      (catch Exception _ value))

    ;; Timestamp-like string with time component
    (re-matches #"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}" value)
    (try
      (.parse (java.text.SimpleDateFormat. "yyyy-MM-dd HH:mm:ss") value)
      (catch Exception _ value))

    (= "\\N" value) nil

    :else value))

(defn parse-record [record]
  (into {} (map (fn [[k v]] [k (parse-value v)]) record)))

(defn is-join-table? [table-name]
  (let [parts (str/split table-name #"_")]
    (= 2 (count parts))))

(defn generate-record [table-name line column-names]
  (let [values (str/split line #"\t")
        column-names (map str/lower-case column-names)
        raw-record (zipmap column-names values)
        parsed-record (parse-record raw-record)]
    (if-not (= (count values) (count column-names))
      (throw (ex-info "Column count mismatch" {:table table-name
                                               :expected column-names
                                               :actual values})))
    (cond
      (= table-name "hits")
      (let [composite-id (str (get parsed-record "counterid") "_"
                               (get parsed-record "eventdate") "_"
                               (get parsed-record "userid") "_"
                               (get parsed-record "eventtime") "_"
                               (get parsed-record "watchid"))]
        (assoc parsed-record "_id" composite-id))

      (is-join-table? table-name)
      (let [[tbl1 tbl2] (str/split table-name #"_")
            tbl1-id (get parsed-record (str tbl1 "_id"))
            tbl2-id (get parsed-record (str tbl2 "_id"))]
        (assoc parsed-record "_id" (str tbl1-id "_" tbl2-id)))

      :else
      (let [tbl-id-key (str table-name "_id")]
        (if-let [tbl-id (get parsed-record tbl-id-key)]
          (-> parsed-record
              (dissoc tbl-id-key)
              (assoc "_id" tbl-id))
          parsed-record)))))

(def hits-header
  ["WatchID" "JavaEnable" "Title" "GoodEvent" "EventTime" "EventDate" "CounterID" "ClientIP" "RegionID" "UserID"
   "CounterClass" "OS" "UserAgent" "URL" "Referer" "IsRefresh" "RefererCategoryID" "RefererRegionID" "URLCategoryID"
   "URLRegionID" "ResolutionWidth" "ResolutionHeight" "ResolutionDepth" "FlashMajor" "FlashMinor" "FlashMinor2"
   "NetMajor" "NetMinor" "UserAgentMajor" "UserAgentMinor" "CookieEnable" "JavascriptEnable" "IsMobile" "MobilePhone"
   "MobilePhoneModel" "Params" "IPNetworkID" "TraficSourceID" "SearchEngineID" "SearchPhrase" "AdvEngineID" "IsArtifical"
   "WindowClientWidth" "WindowClientHeight" "ClientTimeZone" "ClientEventTime" "SilverlightVersion1" "SilverlightVersion2"
   "SilverlightVersion3" "SilverlightVersion4" "PageCharset" "CodeVersion" "IsLink" "IsDownload" "IsNotBounce" "FUniqID"
   "OriginalURL" "HID" "IsOldCounter" "IsEvent" "IsParameter" "DontCountHits" "WithHash" "HitColor" "LocalEventTime" "Age"
   "Sex" "Income" "Interests" "Robotness" "RemoteIP" "WindowName" "OpenerName" "HistoryLength" "BrowserLanguage"
   "BrowserCountry" "SocialNetwork" "SocialAction" "HTTPError" "SendTiming" "DNSTiming" "ConnectTiming"
   "ResponseStartTiming" "ResponseEndTiming" "FetchTiming" "SocialSourceNetworkID" "SocialSourcePage" "ParamPrice"
   "ParamOrderID" "ParamCurrency" "ParamCurrencyID" "OpenstatServiceName" "OpenstatCampaignID" "OpenstatAdID"
   "OpenstatSourceID" "UTMSource" "UTMMedium" "UTMCampaign" "UTMContent" "UTMTerm" "FromTag" "HasGCLID" "RefererHash"
   "URLHash" "CLID"])

(def hits-header-lower (mapv str/lower-case hits-header))

(defn insert-tsv-into-db! [conn table-name file-path]
  (log/info "Processing file for table:" table-name)
  (with-open [reader (io/reader file-path)]
    (let [maybe-header-line (when-not (= table-name "hits") (.readLine reader))
          header (if (= table-name "hits")
                   hits-header-lower
                   (when maybe-header-line
                     (vec (map str/lower-case (str/split maybe-header-line #"\t")))))
          batch-size 1000]
      (if (nil? header)
        (log/warn "File is empty or missing header, skipping table:" table-name)
        (loop [batch [] total-count 0]
          (if-let [line (.readLine reader)]
            (let [record (generate-record table-name line header)
                  batch' (conj batch record)]
              (if (< (count batch') batch-size)
                (recur batch' total-count)
                (do
                  (jdbc/with-transaction [tx conn]
                    (with-open [ps (jdbc/prepare tx [(str "INSERT INTO " table-name " RECORDS ?")])]
                      (jdbc/execute-batch! ps (map vector (map xt-jdbc/->pg-obj batch')))))
                  (recur [] (+ total-count (count batch'))))))
            (do
              (when (seq batch)
                (jdbc/with-transaction [tx conn]
                  (with-open [ps (jdbc/prepare tx [(str "INSERT INTO " table-name " RECORDS ?")])]
                    (jdbc/execute-batch! ps (map vector (map xt-jdbc/->pg-obj batch))))))
              (let [final-total (+ total-count (count batch))]
                (log/debug "Finished inserting" final-total "records for table:" table-name)
                final-total))))))))

(defn process-tsv-files [conn dir]
  (let [tsv-files (read-tsv-files dir)]
    (if (empty? tsv-files)
      (log/warn "No TSV files found in directory:" dir)
      (doseq [[table-name file-path] tsv-files]
        (log/info "Inserting data for table:" table-name "from file:" file-path)
        (try
          (insert-tsv-into-db! conn table-name file-path)
          (log/info "Finished inserting data for table:" table-name)
          (catch Exception e
            (log/error e "Error inserting data for table:" table-name)))))))

(defn -main [& args]
  (if (not (seq args))
    (log/error "Usage: clj -M -m main <path-to-tsv-directory>")
    (let [dir (first args)
          conn (jdbc/get-datasource db-spec)]
      (log/info "Processing TSV files from directory:" dir)
      (process-tsv-files conn dir)
      (log/info "All TSV files have been processed."))))

