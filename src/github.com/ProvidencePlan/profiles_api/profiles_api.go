/*
    TODO:
    * Float64 in JSON are in Scientific Notation which is fine by JSON spec[x]
    * Get Geometry alone by ID
    * Caching via Redis
    * Do we need to dish out Requests to Go Routines?
    * Document What you have learned
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
    //"reflect"

)

type CONFIG struct {
        DB_HOST string
        DB_PORT string
        DB_NAME string
        DB_USER string
        DB_PASS string
}

func getData(ind string, time string, raw_geos string, conf CONFIG) []byte {
    // TODO: Caching
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
        log.Fatal(err)
    }
    defer stmt.Close()

    rows, err := stmt.Query(ind, cleaned_time)
    if err != nil {
        log.Fatal(err)
    }
    results := []interface{}{}
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
    return j
}

func getDataGeoJson(ind string, time string, raw_geos string, conf CONFIG) []byte {
    // The GeoJSON version of this
    // TODO: Caching
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
        log.Fatal(err)
    }
    defer stmt.Close()

    rows, err := stmt.Query(ind, cleaned_time)
    if err != nil {
        log.Fatal(err)
    }

    results := []interface{}{}

    for rows.Next() {
        jrow := make(map[string]interface{})
        err := rows.Scan(&indicator_slug, &display_title, &geography_id, &geography_name, &geometry_id, &value_type, &time_key, &number, &percent, &moe, &f_number, &f_percent, &f_moe, &geom)
        if err != nil {
            log.Fatal(err)
        }
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

    data["objects"] = &results
    j, err := json.Marshal(data)
    return j
}

func getGeomById(c chan map[string]interface{}, id int64, conf CONFIG) {
    // TODO: Caching!
    var geom string
    db, err := getDB(conf)
	if err != nil {
		log.Println("Error trying to call getDB")
        // TODO: How to do we return out of this channel?
	}
    defer db.Close()

    rows, err := db.Query("SELECT ST_AsGeoJSON(geom) FROM maps_polygonmapfeature WHERE id= $1 LIMIT 1", id)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    for rows.Next() {
        err := rows.Scan(&geom)
        if err != nil {
            log.Fatal(err)
        }
    }

    err = rows.Err()

    if err != nil {
        log.Fatal(err)
    }
    geoObj := jsonLoads(geom)
    props := map[string]interface{}{}
    geoObj["properties"] = props
    c <- geoObj
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
                return 405, "time and geos is Required"
            } else if r == "500" {
                return 500, "Server Error"
            } else {
                return 200, r 
            }
        }

    })

    http.ListenAndServe(settings_port, m)
    //m.Run()
}
