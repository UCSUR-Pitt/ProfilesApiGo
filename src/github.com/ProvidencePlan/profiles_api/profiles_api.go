/*
    TODO:
    * Float64 in JSON are in Scientific Notation which is fine by JSON spec
    * Correctly Formatted GeoJSON <-- DONE
    * Abstract DB connection for configuration via some sort of file
    * Caching via Redis
    * Indicator Details in response?
    * Need to make response smaller, possible pull out indicator details from each object

*/
package main

import (
    "fmt"
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
    //"reflect"

)

func main() {
    // ex: http://profiles.provplan.org/maps_api/v1/geo/set/12817/?&name=Total%20Population&time=2010&format=json&limit=0
    // Every request needs to know Indicator, Time, Geography
    // Optionally we can return the geography
    m:= martini.Classic()
    m.Get("/indicator/:slug", func(res http.ResponseWriter, req *http.Request, params martini.Params) (int, string) {
        res.Header().Set("Content-Type", "application/json")
        ind := params["slug"]
        time := req.FormValue("time")
        raw_geos := req.FormValue("geos")
        getGeom := req.FormValue("geom") // should we get the geos?
        var r []byte
        if (getGeom == ""){
            r = getData(ind, time, raw_geos, false)
            r := string(r[:])
            if r != "405" {
                return 200, r
            }else{
                return 405, "time and geos is Required"
            }
        }else{
            // include geoms
            r = getData(ind, time, raw_geos, true)
            r := string(r[:])

            if r != "405" {
                return 200, r
            }else{
                return 405, "time and geos is Required"
            }

        }

    })

    m.Run()
}

func getData(ind string, time string, raw_geos string, get_geoms bool) []byte {
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
    db, err := sql.Open("postgres", "user=asmedrano dbname=cp_pitts_dev") // TODO:Abstract getting db
	if err != nil {
		log.Fatal(err)
	}
    defer db.Close()

    var query string


    if cleaned_geos == "*"{
        query = "SELECT indicator_slug, display_title, geography_id, geography_name, geometry_id, value_type, time_key, number, percent, moe, f_number, f_percent, f_moe FROM profiles_flatvalue WHERE indicator_slug = $1 AND time_key= $2"

    }else{
        query = "SELECT indicator_slug, display_title, geography_id, geography_name, geometry_id, value_type, time_key, number, percent, moe, f_number, f_percent, f_moe FROM profiles_flatvalue WHERE indicator_slug = $1 AND time_key= $2 AND geography_id IN (" +cleaned_geos + ")"
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
        if get_geoms == true{
            // Using a Channel here is totally pointless. Why not just a join? Anyway. Just practicing I guess.
            c := make(chan map[string]interface{})
            if geometry_id.Valid {
                go getGeomById(c, geometry_id.Int64)
                jrow["geom"] = <-c
            }else{
                jrow["geom"] = nil
            }
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

        if get_geoms==true {
            // patch the geojson now if its available 
            if jrow["geom"] != nil{
                jrow["geom"].(map[string]interface{})["properties"].(map[string]interface{})["label"] = geography_name
                v:= map[string]interface{}{}
                v["number"] = jrow["number"]
                v["f_number"] = jrow["f_number"]
                v["percent"] = jrow["percent"]
                v["f_percent"] = jrow["f_percent"]
                v["moe"] = jrow["moe"]
                v["f_moe"] = jrow["f_moe"]
                jrow["geom"].(map[string]interface{})["properties"].(map[string]interface{})["values"] = &v
            }
        }

        results = append(results, jrow)
    }
    data["objects"] = &results
    j, err := json.Marshal(data)
    return j
}


func getGeomById(c chan map[string]interface{}, id int64) {
    // TODO: Caching!
    var geom string
    db, err := sql.Open("postgres", "user=asmedrano dbname=cp_pitts_dev")
	if err != nil {
		log.Fatal(err)
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


/* UTILS */

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
