
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

)

func main() {
    // ex: http://profiles.provplan.org/maps_api/v1/geo/set/12817/?&name=Total%20Population&time=2010&format=json&limit=0
    // Every request needs to know Indicator, Time, Geography
    // Optionally we can return the geography
    m:= martini.Classic()
    m.Get("/api/", func(res http.ResponseWriter, req *http.Request, params martini.Params) string{
        res.Header().Set("Content-Type", "application/json")
        ind := req.FormValue("ind")
        time := req.FormValue("time")
        raw_geos := req.FormValue("geos")
        getGeom := req.FormValue("geom") // should we get the geos?
        //fmt.Println(getGeom)
        var r []byte
        if (getGeom == ""){
            r = getData(ind, time, raw_geos)
        }else{
            // include geoms
        }

        return string(r[:])
    })

    m.Run()
}

func getData(ind string, time string, raw_geos string) []byte {
    // profiles_flatvalue is our table

    var (
        indicator_id int
        display_title string
        geography_id int
        value_type string
        time_key string
        number sql.NullFloat64
        percent sql.NullFloat64
        moe sql.NullFloat64
        f_number sql.NullString
        f_percent sql.NullString
        f_moe sql.NullString
    )
    //geos := strings.Split(raw_geos, ",") // Do commas make sense?
    db, err := sql.Open("postgres", "user=asmedrano dbname=cp_from_dev") // TODO:Abstract getting db
	if err != nil {
		log.Fatal(err)
	}
    defer db.Close()
    var query string
    if raw_geos == "*"{
        query = "SELECT indicator_id, display_title, geography_id, value_type, time_key, number, percent, moe, f_number, f_percent, f_moe FROM profiles_flatvalue WHERE display_title = $1 AND time_key= $2"

    }else{
        query = "SELECT indicator_id, display_title, geography_id, value_type, time_key, number, percent, moe, f_number, f_percent, f_moe FROM profiles_flatvalue WHERE display_title = $1 AND time_key= $2 AND geography_id IN (" +raw_geos + ")"
    }

    stmt, err := db.Prepare(query)
    if err != nil {
        log.Fatal(err)
    }
    defer stmt.Close()

    rows, err := stmt.Query(ind, time)
    if err != nil {
        log.Fatal(err)
    }
    results := []interface{}{}
    for rows.Next() {
        jrow := make(map[string]interface{})
        err := rows.Scan(&indicator_id, &display_title, &geography_id, &value_type, &time_key, &number, &percent, &moe, &f_number, &f_percent, &f_moe)
        if err != nil {
            log.Fatal(err)
        }
        jrow["indicator_id"] = indicator_id
        jrow["display_title"] = display_title
        jrow["geography_id"] = geography_id
        jrow["value_type"] = value_type
        jrow["time_key"] = time_key
        if number.Valid{
            jrow["number"] = number.Float64
        }else{
            jrow["number"] = nil
        }
        if percent.Valid{
            jrow["percent"] = percent.Float64
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

        if f_percent.Valid{
            jrow["f_percent"] = f_percent.String

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

    j, err := json.Marshal(results)
    return j
}


func getGeom() {
    var id int
    var geom string
    db, err := sql.Open("postgres", "user=asmedrano dbname=cp_from_dev")
	if err != nil {
		log.Fatal(err)
	}
    defer db.Close()

    rows, err := db.Query("SELECT id, ST_AsGeoJSON(geom) FROM maps_polygonmapfeature LIMIT 1")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    for rows.Next() {
        err := rows.Scan(&id, &geom)
        if err != nil {
            log.Fatal(err)
        }
        log.Println(id, geom)
    }

    err = rows.Err()

    if err != nil {
        log.Fatal(err)
    }
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
