/*
    TODO:
    * Document What you have learned
    * Allow for the rest of the shapefile types
    * Use file logger
    * Add Way list indicator slugs
    * ADD CORS info to config

    URL Examples:
    indicator data
    [host]/[slug]?time=<validtime>&geos=<id,id,id>&geoms=t<optional_geojson>
    127.0.0.1:8080/indicator/total-population?time=2000&geos=*

    shpfiles
    [host]/shp/?geoms=1,3,4,5<ids> Arbitrary Geometry Id TODO: accomodate different types ( POINT, LINE )
    
    shpfiles geoquery
    Ex: get geoms in geom(419) where lev = 6
    [host]shp/q/?geoms=419&lev=6&q=IN

    get geography by level
    [host]/geos/level/:slug

*/
package main

import (
    "fmt"
    "os"
    "strings"
    "log"
    "github.com/codegangsta/martini"
    _ "github.com/lib/pq"
    "database/sql"
    "net/http"
    "strconv"
    "encoding/json"
    "regexp"
    "errors"
    "io/ioutil"
    "github.com/ProvidencePlan/profiles_api/cache"

)

type CONFIG struct {
        DB_HOST string
        DB_PORT string
        DB_NAME string
        DB_USER string
        DB_PASS string
        REDIS_CONN string //host and port ex: 127.0.0.1:6379
        CACHE_EXPIRE int
}

func getFromCache(connStr string, hash string) ([]byte) {
    rConn, err := cache.RedisConn(connStr)
    if err == nil{
        defer rConn.Close()
        r , err := cache.GetKey(rConn, hash) // if this is empty, r is a empty byte len(r) == 0
        if err == nil {
            return r
        }
    }

    var b []byte
    return b
}

func putInCache(connStr string, hash string, val []byte, expire int) {
    rConn, err := cache.RedisConn(connStr)
    if err == nil {
        cache.SetKey(rConn, hash, val, expire)
        defer rConn.Close()
    }

}


// grab meta data for given ind in a new go routine
func getMetaData(c chan []interface{}, db *sql.DB, ind_slug string) {
    var (
        indicator_id int
        slug string
        display_title string
        time_key string
    )

    id_query := "SELECT indicator_id from profiles_flatvalue WHERE indicator_slug =$1 LIMIT 1";
    err := db.QueryRow(id_query, ind_slug).Scan(&indicator_id)
    if err != nil {
        log.Println("Error preparing query %s in getMetaData", id_query)
    }
    query := "SELECT DISTINCT indicator_slug, display_title, time_key from profiles_flatvalue WHERE indicator_id=$1;"
    stmt, err := db.Prepare(query)
    if err != nil {
        log.Println("Error preparing query %s in getMetaData", query)
    }
    defer stmt.Close()
    rows, err := stmt.Query(indicator_id)
    if err != nil {
        log.Println("Error running query %s in getMetaData", query)
    }
    results := []interface{}{}
    for rows.Next() {
        mrow := make(map[string]interface{})
        err := rows.Scan(&slug, &display_title, &time_key)
        if err != nil {
            log.Println("Error running query %s in getMetaData", query)
        }
        mrow["slug"] = slug
        mrow["title"] = display_title
        mrow["time_key"] = time_key
        results = append(results, mrow)
    }

    c <- results

}

// Indicator Api Handler. Returns values only
func getData(ind string, time string, raw_geos string, conf CONFIG) []byte {
    hash := cache.MakeHash(ind + time + raw_geos)
    c := getFromCache(conf.REDIS_CONN, hash)
    if len(c) != 0{
        log.Println("Serving getData from cache: ", ind + time + raw_geos)
        return c
    }

    var (
        indicator_slug string
        display_title string
        geography_id int
        geography_name string
        //geography_slug string
        geometry_id sql.NullInt64
        value_type string
        time_key string
        number sql.NullFloat64
        percent sql.NullFloat64
        moe sql.NullFloat64
        f_number sql.NullString
        f_percent sql.NullString
        f_moe sql.NullString
    )

    /* SANITIZING INPUTS */
    cleaned_geos, err := sanitize(raw_geos, "[0-9,\\*]+")
    if err != nil{
        r:=[]byte("405")
        return r
    }

    cleaned_time, err := sanitize(time, "[0-9,\\*]+")
    if err != nil{
        r:=[]byte("405")
        return r

    }
    
    data := map[string]interface{}{} // this will be the object that wraps everything

    //geos := strings.Split(raw_geos, ",") // Do commas make sense?
    //db, err := sql.Open("postgres", "user=asmedrano dbname=cp_pitts_dev") // TODO:Abstract getting db
    db, err := getDB(conf)
	if err != nil {
		log.Println("Error trying to call getDB")
        r:=[]byte("500")
        return r
	}
    defer db.Close()

    var base_query string = "SELECT indicator_slug, display_title, geography_id, geography_name, geometry_id, value_type, time_key, number, percent, moe, f_number, f_percent, f_moe FROM profiles_flatvalue WHERE indicator_slug = $1 AND time_key= $2"
    var query string

    // we need to support getting * geos or specific ones via thier ids
    if cleaned_geos == "*"{
        query = base_query
    }else{
        query = base_query + "AND geography_id IN (" +cleaned_geos + ")"
    }

    stmt, err := db.Prepare(query)
    if err != nil {
        log.Println("Error preparing query %s in getData", query)
    }
    defer stmt.Close()

    rows, err := stmt.Query(ind, cleaned_time)
    if err != nil {
        log.Println("Error running query %s in getData", query)
    }

    results := []interface{}{}
    metachan := make(chan []interface{}) // get the meta data
    go getMetaData(metachan, db, ind)
    data["related"] = <- metachan

    for rows.Next() {
        jrow := make(map[string]interface{})
        err := rows.Scan(&indicator_slug, &display_title, &geography_id, &geography_name, &geometry_id, &value_type, &time_key, &number, &percent, &moe, &f_number, &f_percent, &f_moe)
        if err != nil {
            log.Fatal(err)
        }
        jrow["indicator_slug"] = indicator_slug
        jrow["display_title"] = display_title
        jrow["geography_id"] = geography_id
        jrow["geography_name"] = geography_name
        jrow["value_type"] = value_type
        jrow["time_key"] = time_key
        if number.Valid{
            jrow["number"] = number.Float64
        }else{
            jrow["number"] = nil
        }
        if value_type != "i"{
            if percent.Valid{
                jrow["percent"] = percent.Float64
            }else{
                jrow["percent"] = nil
            }
        }else{
            jrow["percent"] = nil
        }
        if moe.Valid{
            jrow["moe"] = moe.Float64
        }else{
            jrow["moe"] = nil
        }

        if f_number.Valid{
            jrow["f_number"] = f_number.String

        }else{
            jrow["f_number"] = nil
        }
        if value_type != "i"{
            if f_percent.Valid{
                jrow["f_percent"] = f_percent.String

            }else{
                jrow["f_percent"] = nil
            }
        }else{
            jrow["f_percent"] = nil
        }

        if f_moe.Valid{
            jrow["f_moe"] = f_moe.String

        }else{
            jrow["f_moe"] = nil
        }

        results = append(results, jrow)
    }
    data["objects"] = &results

    j, err := json.Marshal(data)

    putInCache(conf.REDIS_CONN, hash, j, conf.CACHE_EXPIRE)

    return j
}

func getDataGeoJson(ind string, time string, raw_geos string, conf CONFIG) []byte {
    // The GeoJSON version of this
    hash := cache.MakeHash("gdgj:" + ind + time + raw_geos)
    c := getFromCache(conf.REDIS_CONN, hash)
    if len(c) != 0{
        log.Println("Serving getData from cache gdgj: ", ind + time + raw_geos)
        return c
    }
    var (
        //indicator_id int
        indicator_slug string
        display_title string
        geography_id int
        geography_name string
        //geography_slug string
        geometry_id sql.NullInt64
        value_type string
        time_key string
        number sql.NullFloat64
        percent sql.NullFloat64
        moe sql.NullFloat64
        f_number sql.NullString
        f_percent sql.NullString
        f_moe sql.NullString
        geom sql.NullString
    )

    /* SANITIZING INPUTS */
    cleaned_geos, err := sanitize(raw_geos, "[0-9,\\*]+")
    if err != nil{
        r:=[]byte("405")
        return r
    }

    cleaned_time, err := sanitize(time, "[0-9,\\*]+")
    if err != nil{
        r:=[]byte("405")
        return r

    }
    
    data := map[string]interface{}{} // this will be the object that wraps everything

    db, err := getDB(conf)
	if err != nil {
		log.Println("Error trying to call getDB")
        r:=[]byte("500")
        return r
	}
    defer db.Close()
    var base_query = "SELECT profiles_flatvalue.indicator_slug, profiles_flatvalue.display_title, profiles_flatvalue.geography_id, profiles_flatvalue.geography_name, profiles_flatvalue.geometry_id, profiles_flatvalue.value_type, profiles_flatvalue.time_key, profiles_flatvalue.number, profiles_flatvalue.percent, profiles_flatvalue.moe, profiles_flatvalue.f_number, profiles_flatvalue.f_percent, profiles_flatvalue.f_moe, ST_AsGeoJSON(maps_polygonmapfeature.geom) AS geom FROM profiles_flatvalue LEFT OUTER JOIN maps_polygonmapfeature ON (maps_polygonmapfeature.id = profiles_flatvalue.geometry_id) WHERE profiles_flatvalue.indicator_slug = $1 AND time_key= $2"
    var query string 

    // we need to support getting * geos or specific ones via thier ids also we need to be able to join on a geom
    if cleaned_geos == "*"{
        query = base_query

    }else{
        query = base_query + "AND profile_flatvalue.geography_id IN (" +cleaned_geos + ")"
    }

    stmt, err := db.Prepare(query)
    if err != nil {
        log.Println("Error runnning query: getDataGeoJson")
        r:=[]byte("500")
        return r

    }
    defer stmt.Close()

    rows, err := stmt.Query(ind, cleaned_time)
    if err != nil {
        log.Fatal(err)
    }

    results := []interface{}{}
    metachan := make(chan []interface{}) // get the meta data
    go getMetaData(metachan, db, ind)
    data["related"] = <- metachan

    for rows.Next() {
        jrow := make(map[string]interface{})
        err := rows.Scan(&indicator_slug, &display_title, &geography_id, &geography_name, &geometry_id, &value_type, &time_key, &number, &percent, &moe, &f_number, &f_percent, &f_moe, &geom)
        if err == nil {
            if geom.Valid{
                jrow = jsonLoads(geom.String)
            }else{
                jrow["coordinates"] = nil
            }
            properties := make(map[string]interface{})
            properties["label"] = geography_name
            properties["geography_id"] = geography_id
            values := make(map[string]interface{})
            values["indicator_slug"] = indicator_slug
            values["value_type"] = value_type
            values["time_key"] = time_key
            if number.Valid{
                values["number"] = number.Float64
            }else{
                values["number"] = nil
            }
            if value_type != "i"{
                if percent.Valid{
                    values["percent"] = percent.Float64
                }else{
                    values["percent"] = nil
                }
            }else{
                values["percent"] = nil
            }
            if moe.Valid{
                values["moe"] = moe.Float64
            }else{
                values["moe"] = nil
            }

            if f_number.Valid{
                values["f_number"] = f_number.String

            }else{
                values["f_number"] = nil
            }
            if value_type != "i"{
                if f_percent.Valid{
                    values["f_percent"] = f_percent.String

                }else{
                    values["f_percent"] = nil
                }
            }else{
                values["f_percent"] = nil
            }

            if f_moe.Valid{
                values["f_moe"] = f_moe.String

            }else{
                values["f_moe"] = nil
            }

            jrow["properties"] = &properties
            jrow["values"] = &values

            results = append(results, jrow)
        }
    }

    data["objects"] = &results
    j, err := json.Marshal(data)

    putInCache(conf.REDIS_CONN, hash, j, conf.CACHE_EXPIRE)

    return j
}


// Get Geoms by a list of geometry ids
func getGeomsById(geoms_ids string, conf CONFIG) []byte {
    /*
        geoms_ids is a comma delimited string
    */
    hash := cache.MakeHash("shp:" + geoms_ids)
    c := getFromCache(conf.REDIS_CONN, hash)
    if len(c) != 0{
        log.Println("Serving getData from shp cache:", geoms_ids)
        return c
    }

    var (
        geom string
        geo_key string
        label string
    )

    cleaned_geos, err := sanitize(geoms_ids, "[0-9,\\*]+")

    if err != nil{
        r:=[]byte("405")
        return r
    }
    db, err := getDB(conf)
	if err != nil {
		log.Println("Error trying to call getDB")
        r:=[]byte("500")
        return r
	}
    defer db.Close()


    rows, err := db.Query("SELECT geo_key, label, ST_AsGeoJSON(geom) FROM maps_polygonmapfeature WHERE id IN (" + cleaned_geos + ")")

    if err != nil {
        log.Println("Error runnning query: getGeomsById")
        r:=[]byte("500")
        return r
    }
    defer rows.Close()
    data := map[string]interface{}{} // this will be the object that wraps everything
    results := []interface{}{}
    for rows.Next() {
        err := rows.Scan(&geo_key, &label, &geom)
        if err == nil {
            properties := make(map[string]interface{})
            properties["geo_key"] = &geo_key
            properties["label"] = &label
            geom := jsonLoads(geom)
            geom["properties"] = &properties

            results = append(results, geom)
        }
    }
    data["objects"] = &results
    j, err := json.Marshal(data)
    putInCache(conf.REDIS_CONN, hash, j, conf.CACHE_EXPIRE)
    return j
}


// Return a list of polygons that are IN or OF geom_id depending on what the geo_lev_id is
/* Example:
    IN query
    Find geoms of geolevel 6 that are in geom 419
    http://127.0.0.1:8080/shp/q/?geoms=419&lev=6&q=IN
    
    OF query
    Find geoms of geolevel 1 that contain geoms 380
    http://127.0.0.1:8080/shp/q/?geoms=380&lev=1&q=OF

*/
func getGeoQuery(conf CONFIG, geoms_ids string, geo_lev_id string, query_type string) []byte {

    hash := cache.MakeHash("gGQ:" + geoms_ids + geo_lev_id + query_type)
    c := getFromCache(conf.REDIS_CONN, hash)
    if len(c) != 0{
        log.Println("Serving getData from cache: ",  "gGQ:" + geoms_ids + geo_lev_id + query_type)
        return c
    }

    cleaned_geoms, err := sanitize(geoms_ids, "[0-9,\\*]+")
    if err != nil{
        r:=[]byte("405")
        return r
    }

    cleaned_geo_lev, err := sanitize(geo_lev_id, "[0-9]+")
    if err != nil{
        r:=[]byte("405")
        return r
    }

    cleaned_query_type, err := sanitize(query_type, "(IN|OF)")
    if err != nil{
        r:=[]byte("405")
        return r
    }
    // also make sure the sanitized query_Type is IN or OF
    if cleaned_query_type != "IN" && cleaned_query_type != "OF" {
        r:=[]byte("405")
        return r
    }

    // First find the geom and join all the geometries
    var (
        geomId int
        geoKey string
        label string
    )
    var geom_query string
    if cleaned_query_type == "IN" {
        // find geoms contained in this geom
        geom_query = "WITH targ_levs AS (SELECT id, geo_key, geom, label FROM maps_polygonmapfeature WHERE geo_level=$1), targ_geom AS (SELECT id, geo_key, geom FROM maps_polygonmapfeature WHERE id IN (" + cleaned_geoms + ") LIMIT 1) SELECT targ_levs.id, targ_levs.geo_key, targ_levs.label FROM targ_levs, targ_geom WHERE ST_Contains(targ_geom.geom, ST_Centroid(targ_levs.geom))"
    }else if cleaned_query_type == "OF"{
        // find geoms that contain geom
        geom_query = "WITH levs AS (SELECT id, geo_key, geom, label FROM maps_polygonmapfeature WHERE geo_level=$1), targ AS (SELECT id, geo_key, geom FROM maps_polygonmapfeature WHERE id IN (" + cleaned_geoms + ") LIMIT 1) SELECT levs.id, levs.geo_key, levs.label FROM levs, targ WHERE ST_Contains(levs.geom, ST_Centroid(targ.geom))"

    }

    //TODO: We tend to always run querires like this, why not abstract it
    db, err := getDB(conf)
    if err != nil {
        log.Println("Error trying to call getDB")
        r:=[]byte("500")
        return r
    }
    defer db.Close()
    stmt, err := db.Prepare(geom_query)
    if err != nil {
        log.Println("Error preparing query: ", geom_query)
    }
    defer stmt.Close()

    rows, err := stmt.Query(cleaned_geo_lev)
    if err != nil {
        log.Println("Error running query ", geom_query)
    }

    data := map[string]interface{}{} // this will be the object that wraps everything    
    results := []interface{}{}
    for rows.Next() {
        jrow := make(map[string]interface{})
        err := rows.Scan(&geomId, &geoKey, &label)
        if err == nil {
            jrow["id"] = geomId
            jrow["geoKey"] = geoKey
            jrow["label"] = label
            results = append(results, jrow)
        }else{
            log.Println("Error in row: %s ", err)
        }
    }
    data["objects"] = &results
    j, err := json.Marshal(data)

    putInCache(conf.REDIS_CONN, hash, j, conf.CACHE_EXPIRE)

    return j

}

// Return GeoRecords by Level slug
func getGeosByLevSlug(conf CONFIG, levslug string) []byte {
    hash := cache.MakeHash("gGByLS:" + levslug)

    c := getFromCache(conf.REDIS_CONN, hash)
    if len(c) != 0{
        log.Println("Serving getData from cache: ",  "gGbyLS:" + levslug )
        return c
    }

    var (
        id int
        slug string
        name string
    )

    cleaned_slug, err := sanitize(levslug, "[-\\w]+")
    if err != nil{
        r:=[]byte("405")
        return r
    }

    db, err := getDB(conf)
	if err != nil {
		log.Println("Error trying to call getDB")
        r:=[]byte("500")
        return r
	}
    defer db.Close()
    
    query := "SELECT profiles_georecord.geo_id, profiles_georecord.slug, profiles_georecord.name FROM profiles_geolevel FULL JOIN profiles_georecord ON profiles_georecord.level_id = profiles_geolevel.id WHERE profiles_geolevel.slug=$1"
    
    rows, err := db.Query(query, cleaned_slug)
    if err != nil {
        log.Println("Error runnning query: %s", query)
        r:=[]byte("500")
        return r
    }
    defer rows.Close()

    data := map[string]interface{}{} // this will be the object that wraps everything
    results := []interface{}{}
    for rows.Next() {
        err := rows.Scan(&id, &slug, &name)
        if err == nil {
            jrow := make(map[string]interface{})
            jrow["geoKey"] = id
            jrow["slug"]= slug
            jrow["name"]= name
            results = append(results, jrow)
        }
    }

    data["objects"] = &results
    j, err := json.Marshal(data)

    putInCache(conf.REDIS_CONN, hash, j, conf.CACHE_EXPIRE)

    return j
}


/* --------------------UTILS -----------------*/

func sanitize(input string, allowed string) (res string, err error) {
    /*
        input: the string to check
        allowed: a regex string

        Returns error if match fails
        nil otherwise
    */
    matched, err := regexp.MatchString(allowed, input)
    if err != nil {
        return "", errors.New("405")
    }

    if matched == false {
        return "", errors.New("405")
    }
    // since we know we have a match, lets pull the string out
    re := regexp.MustCompile(allowed)
    output := re.FindString(input)
    // Trim things that will break things
    output = strings.TrimPrefix(output, ",")
	output = strings.TrimSuffix(output, ",")
    output = strings.TrimPrefix(output, "-")
	output = strings.TrimSuffix(output, "-")

    return output, nil
}

func jsonLoads(j string) map[string]interface{} {
    sB := []byte(j)
    var f interface{}
    // At this point the Go value in f would be a map whose keys are strings and whose values are themselves stored as empty interface values:
    // If the json is formated wrong, f will be nil :TODO catch that error
    json.Unmarshal(sB, &f)
    // To access this data we can use a type assertion to access `f`'s underlying map[string]interface{}:
    m := f.(map[string]interface{})
    return m
}


func csToIs(cs string) []int {
    st := strings.Split(cs, ",")
    out := []int{}
    for i:=0; i< len(st); i++ {
        val, err:= strconv.Atoi(st[i])
        if err == nil{
            out = append(out, val)
        }
    }
    return out
}

func print(args ...interface{}) {
    fmt.Println(args...)
}

func getConf(conf_path string) CONFIG {
    path := conf_path
    file, err := ioutil.ReadFile(path)
    if err != nil {
            log.Fatalf("Could not open file '%s'.", path)
            os.Exit(1)
    }
    conf := CONFIG{}
    json.Unmarshal(file, &conf)
    return conf
}

func getDB(conf CONFIG) (*sql.DB, error)  {
    conn_str := fmt.Sprintf("user=%s dbname=%s host=%s port=%s password=%s  sslmode=disable",
    conf.DB_USER, conf.DB_NAME, conf.DB_HOST, conf.DB_PORT, conf.DB_PASS)
    db, err := sql.Open("postgres", conn_str)
    err = db.Ping()
    return db, err
}

func main() {
    // ex: http://profiles.provplan.org/maps_api/v1/geo/set/12817/?&name=Total%20Population&time=2010&format=json&limit=0
    // Every request needs to know Indicator, Time, Geography
    // Optionally we can return the geography
    args := os.Args
    if len(args) < 2 {
        log.Fatal("Config file and port Required. Ex: profiles_api settings.json :8080")
        os.Exit(1)
    }
    settings_path := args[1]
    settings_port := args[2]
    log.Printf("Starting Server on %s", settings_port)
    conf := getConf(settings_path)
    m:= martini.Classic()
    m.Get("/indicator/:slug", func(res http.ResponseWriter, req *http.Request, params martini.Params) (int, string) {
        res.Header().Set("Content-Type", "application/json")
        res.Header().Set("Access-Control-Allow-Origin", "*")
        ind := params["slug"]
        time := req.FormValue("time")
        raw_geos := req.FormValue("geos")
        getGeom := req.FormValue("geom") // should we get the geos?
        var r []byte
        if (getGeom == ""){
            r = getData(ind, time, raw_geos, conf)
            r := string(r[:])
            if r != "405" {
                return 200, r
            }else{
                return 405, "time and geos is Required"
            }
        }else{
            // include geoms
            r = getDataGeoJson(ind, time, raw_geos, conf)
            r := string(r[:])
            if r == "405" {
                return 405, "time and geos Parameters are Required"
            } else if r == "500" {
                return 500, "Server Error"
            } else {
                return 200, r 
            }
        }

    })

    m.Get("/shp/", func (res http.ResponseWriter, req *http.Request) (int, string){
        res.Header().Set("Content-Type", "application/json")
        res.Header().Set("Access-Control-Allow-Origin", "*")
        var r []byte
        geoms := req.FormValue("geoms")
        r = getGeomsById(geoms, conf)
        rs := string(r[:])
        if rs == "405" {
            return 405, "geoms Parameter is Required"
        } else if rs == "500" {
            return 500, "Server Error"
        } else {
            return 200, rs
        }

    })

    m.Get("/shp/q/", func (res http.ResponseWriter, req *http.Request) (int, string){
        res.Header().Set("Content-Type", "application/json")
        res.Header().Set("Access-Control-Allow-Origin", "*")
        var r []byte
        geoms_ids := req.FormValue("geoms")
        geo_lev_id := req.FormValue("lev")
        query_type := req.FormValue("q")
        r = getGeoQuery(conf, geoms_ids, geo_lev_id, query_type)
        rs := string(r[:])
        if rs == "405" {
            return 405, "geoms, lev and q are required "
        } else if rs == "500" {
            return 500, "Server Error"
        } else {
            return 200, rs
        }

    })

    m.Get("/geos/level/:slug", func(res http.ResponseWriter, req *http.Request, params martini.Params)(int, string){
        res.Header().Set("Content-Type", "application/json")
        res.Header().Set("Access-Control-Allow-Origin", "*")
        var r []byte
        r = getGeosByLevSlug(conf, params["slug"])
        rs := string(r[:])
        if rs == "405" {
            return 405, "valid slug is required"
        } else if rs == "500" {
            return 500, "Server Error"
        } else {
            return 200, rs
        }


    });


    http.ListenAndServe(settings_port, m)
}
