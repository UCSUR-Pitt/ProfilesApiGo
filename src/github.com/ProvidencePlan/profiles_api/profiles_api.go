/*
   TODO:
   * Document What you have learned
   * Allow for the rest of the shapefile types
   * Use file logger
   * Add Way list indicator slugs
   * Need to support Multiple Times
   * Query error should return proper exit code
   * STANDARDIZE Using query results. This is not DRY at ALL!

   URL Examples:
   indicator data
   [host]/[slug]?time=<validtime>&geos=<id,id,id>&geom=t<optional_geojson-that will return the geometry>
   127.0.0.1:8080/indicator/total-population?time=2000&geos=*


   shpfiles
   [host]/shp/?geoms=1,3,4,5<ids> Arbitrary Geometry Id TODO: accomodate different types ( POINT, LINE )

   shpfiles geoquery
   Ex: get geoms in a specific geom Ex: 419 where lev = 6
   [host]shp/q/?geoms=419&lev=6&q=IN

   get geographies by level
   [host]/geos/level/:slug
   Optional:
   ?filter=FUZZYMATCH via geokey


*/
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/codegangsta/martini"
	_ "github.com/lib/pq"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	//"encoding/csv"
	"errors"
	"github.com/ProvidencePlan/profiles_api/cache"
	"io/ioutil"
	"regexp"
    "sort"
)

type CONFIG struct {
	DB_HOST      string
	DB_PORT      string
	DB_NAME      string
	DB_USER      string
	DB_PASS      string
	REDIS_CONN   string //host and port ex: 127.0.0.1:6379
	CACHE_EXPIRE int
}

func getFromCache(connStr string, hash string) []byte {
	rConn, err := cache.RedisConn(connStr)
	if err == nil {
		defer rConn.Close()
		r, err := cache.GetKey(rConn, hash) // if this is empty, r is a empty byte len(r) == 0
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
		indicator_id  int
		slug          string
		display_title string
		time_key      string
	)

	id_query := "SELECT indicator_id from profiles_flatvalue WHERE indicator_slug =$1 LIMIT 1"
	err := db.QueryRow(id_query, ind_slug).Scan(&indicator_id)
	if err != nil {
		log.Println("Error running query in getMetaData", id_query, "params:"+ind_slug, err)
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
	if len(c) != 0 {
		log.Println("Serving getData from cache: ", ind+time+raw_geos)
		return c
	}

	var (
		indicator_slug string
		display_title  string
		geography_id   int
		geography_name string
		//geography_slug string
		geometry_id     sql.NullInt64
		value_type      string
		time_key        string
		number          sql.NullFloat64
		percent         sql.NullFloat64
		moe             sql.NullFloat64
		numerator       sql.NullFloat64
		numerator_moe   sql.NullFloat64
		f_number        sql.NullString
		f_percent       sql.NullString
		f_moe           sql.NullString
		f_numerator     sql.NullString
		f_numerator_moe sql.NullString
	)

	/* SANITIZING INPUTS */
	cleaned_geos, err := sanitize(raw_geos, "[0-9,\\*]+")
	if err != nil {
		r := []byte("405")
		return r
	}

	cleaned_time, err := sanitize(time, "[0-9,\\*\\-\\s]+")
	if err != nil {
		r := []byte("405")
		return r
	}

	data := map[string]interface{}{} // this will be the object that wraps everything

	db, err := getDB(conf)
	if err != nil {
		log.Println("Error trying to call getDB")
		r := []byte("500")
		return r
	}
	defer db.Close()

	var base_query string = "SELECT indicator_slug, display_title, geography_id, geography_name, geometry_id, value_type, time_key, number, percent, moe, numerator,numerator_moe, f_number, f_percent, f_moe, f_numerator, f_numerator_moe FROM profiles_flatvalue WHERE indicator_slug = $1 AND time_key= $2"
	var query string

	// we need to support getting * geos or specific ones via thier ids
	if cleaned_geos == "*" {
		query = base_query
	} else {
		query = base_query + "AND geography_id IN (" + cleaned_geos + ")"
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
	data["related"] = <-metachan

	for rows.Next() {
		jrow := make(map[string]interface{})
		err := rows.Scan(&indicator_slug, &display_title, &geography_id, &geography_name, &geometry_id, &value_type, &time_key, &number, &percent, &moe, &numerator, &numerator_moe, &f_number, &f_percent, &f_moe, &f_numerator, &f_numerator_moe)
		if err != nil {
			log.Fatal(err)
		}
		jrow["indicator_slug"] = indicator_slug
		jrow["display_title"] = display_title
		jrow["geography_id"] = geography_id
		jrow["geography_name"] = geography_name
		jrow["value_type"] = value_type
		jrow["time_key"] = time_key
		if number.Valid {
			jrow["number"] = number.Float64
		} else {
			jrow["number"] = nil
		}
		if value_type != "i" {
			if percent.Valid {
				jrow["percent"] = percent.Float64
			} else {
				jrow["percent"] = nil
			}
		} else {
			jrow["percent"] = nil
		}
		if moe.Valid {
			jrow["moe"] = moe.Float64
		} else {
			jrow["moe"] = nil
		}
		if f_number.Valid {
			jrow["f_number"] = f_number.String

		} else {
			jrow["f_number"] = nil
		}
		if value_type != "i" {
			if numerator.Valid {
				jrow["numerator"] = numerator.Float64
			} else {
				jrow["numerator"] = nil
			}
			if f_numerator.Valid {
				jrow["f_numerator"] = f_numerator.String
			} else {
				jrow["f_numerator"] = nil
			}
			if numerator.Valid {
				jrow["numerator_moe"] = numerator.Float64
			} else {
				jrow["numerator_moe"] = nil
			}
			if f_numerator.Valid {
				jrow["f_numerator_moe"] = f_numerator.String
			} else {
				jrow["f_numerator_moe"] = nil
			}
			if f_percent.Valid {
				jrow["f_percent"] = f_percent.String

			} else {
				jrow["f_percent"] = nil
			}

		} else {
			jrow["f_percent"] = nil
		}

		if f_moe.Valid {
			jrow["f_moe"] = f_moe.String

		} else {
			jrow["f_moe"] = nil
		}

		results = append(results, jrow)
	}
	data["objects"] = &results

	j, err := json.Marshal(data)

	if len(results) != 0 {
		putInCache(conf.REDIS_CONN, hash, j, conf.CACHE_EXPIRE)
	}

	return j
}

// TODO: this stuct needs to be implemented everywhere we fetch FlatValue data.
type FlatValue struct {
	Display_title    string
	Geography_name   string
	Geography_geokey string
	Time_key         string
	Number           sql.NullFloat64
	Percent          sql.NullFloat64
	Moe              sql.NullFloat64
	F_number         sql.NullString
	F_percent        sql.NullString
	F_moe            sql.NullString
}

// Returns a Map for each field as a string
func (f *FlatValue) ToMap() map[string]string {
	m := map[string]string{}

	if !f.Number.Valid {
		m["number"] = ""
	} else {
		m["number"] = f.F_number.String
	}
	if !f.Percent.Valid {
		m["percent"] = ""
	} else {
		m["percent"] = f.F_percent.String
	}

	if !f.Moe.Valid {
		m["moe"] = ""
	} else {
		m["moe"] = f.F_moe.String
	}

	m["display_title"] = f.Display_title
	m["time_key"] = f.Time_key
	m["geography_name"] = f.Geography_name

	return m
}

func toSlice(f FlatValue) []string { // TODO: why cant I make this part of the struct. Error is: Cannot call pointer method on val. 
    s := []string{}
    for _, val := range(f.ToMap()) {
        s = append(s, val)
    }
    return s
}


// Write csv formated data to w
// inds is string of indicator ids, raw_goes is a string of geo_ids
// We validate them first and return an error if anything happens
func getDataCSV(res http.ResponseWriter, inds string, raw_geos string, config CONFIG) {

	/* SANITIZING INPUTS */
	cleaned_geos, err := sanitize(raw_geos, "[0-9,]+")
	if err != nil {
		http.Error(res, "", 500)
		return
	}

	cleaned_inds, err := sanitize(inds, "[0-9,]+")
	if err != nil {
		http.Error(res, "", 500)
		return
	}

	// Now we need to decide whether or not we want to give the user a single indicator
	// or a single geography with many indicators
	splitInds := strings.Split(cleaned_inds, ",")
	splitGeos := strings.Split(cleaned_geos, ",")

	if len(splitInds) == 1 {
		// Geos can be N
	} else if len(splitInds) > 1 {
		cleaned_geos = splitGeos[0]
	}

	db, err := getDB(config)
	if err != nil {
		log.Println("Error trying to call getDB--GetCSV")
		http.Error(res, "", 500)
		return
	}
	defer db.Close()

	var time string
	var timeSet []string
	var flatValues = make(map[string]map[string]FlatValue) // {indid: {time1:, time2: time:3}}

   // baseHeaders := []string{"indicator", "group", "geo", "geo_id"}
   // baseDataHeaders := []string{"est", "moe", "pct", "pct_moe"} // each time should get a set of these

	// FETCH the Distinct Times in our Dataset
	timesQ := "SELECT DISTINCT time_key FROM profiles_flatvalue WHERE indicator_id IN (%v) AND geography_id IN(%v) AND time_key != 'change'"
	timesQ = fmt.Sprintf(timesQ, cleaned_inds, cleaned_geos)
	tRows, err := db.Query(timesQ)
	if err != nil {
		fmt.Println(err)
	}
	for tRows.Next() {
		err := tRows.Scan(&time)
		if err != nil {
			log.Fatal(err)
		}
		timeSet = append(timeSet, time)
	}
    sort.Strings(timeSet)
	// Now Fetch the Data
	query := "SELECT display_title, geography_name, geography_geo_key, time_key, number, percent, moe, f_number, f_percent, f_moe FROM profiles_flatvalue WHERE indicator_id IN (%v) AND geography_id IN(%v) AND time_key != 'change' ORDER BY indicator_id"

	query = fmt.Sprintf(query, cleaned_inds, cleaned_geos)

	stmt, err := db.Prepare(query)
	if err != nil {
		log.Println("Error Preparing query: getCSV")
		http.Error(res, "", 500)
		return

	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		v := FlatValue{}
		err := rows.Scan(&v.Display_title, &v.Geography_name, &v.Geography_geokey, &v.Time_key, &v.Number, &v.Percent, &v.Moe, &v.F_number, &v.F_percent, &v.F_moe)

		if err == nil {
			//fmt.Println(v.ToMap())
			// add a new key to our map for each indicator if it doesnt exist and then populate it with all our time posibilities
			_, exists := flatValues[v.Display_title]
			if !exists {

				flatValues[v.Display_title] = make(map[string]FlatValue)
				for _, t := range timeSet {
					flatValues[v.Display_title][t] = FlatValue{Time_key: t}
				}
			}

			// Now actually store values as they come
			flatValues[v.Display_title][v.Time_key] = v
		}

	}

	// at this point our values are prepped for export
    for _, val := range flatValues { // key: Indicicator Name, val: map of times with vals
        csvRow :=[]string{}
        for _, t := range(timeSet){ // t : time_key. Now we are starting to form our csv rows. Which should all contain base Headrs
            //print(val[t]) // val[t] is a FlatValue struct
            //TODO:NEED TO SORT DATA ROWS!
            csvRow = append(csvRow, toSlice(val[t])...)
        }

        print(csvRow)
	}



}

func getDataGeoJson(ind string, time string, raw_geos string, conf CONFIG) []byte {
	// Join indicator data with shapefiles geoms
	hash := cache.MakeHash("gdgj:" + ind + time + raw_geos)
	c := getFromCache(conf.REDIS_CONN, hash)
	if len(c) != 0 {
		log.Println("Serving getData from cache gdgj: ", ind+time+raw_geos)
		return c
	}
	var (
		//indicator_id int
		indicator_slug string
		display_title  string
		geography_id   int
		geography_name string
		//geography_slug string
		geometry_id     sql.NullInt64
		value_type      string
		time_key        string
		number          sql.NullFloat64
		percent         sql.NullFloat64
		moe             sql.NullFloat64
		numerator       sql.NullFloat64
		numerator_moe   sql.NullFloat64
		f_number        sql.NullString
		f_percent       sql.NullString
		f_moe           sql.NullString
		f_numerator     sql.NullString
		f_numerator_moe sql.NullString
		geom            sql.NullString
	)

	/* SANITIZING INPUTS */
	cleaned_geos, err := sanitize(raw_geos, "[0-9,\\*]+")
	if err != nil {
		r := []byte("405")
		return r
	}

	cleaned_time, err := sanitize(time, "[0-9,\\*\\-\\s]+")
	if err != nil {
		r := []byte("405")
		return r

	}

	data := map[string]interface{}{} // this will be the object that wraps everything

	db, err := getDB(conf)
	if err != nil {
		log.Println("Error trying to call getDB")
		r := []byte("500")
		return r
	}
	defer db.Close()
	var base_query = "SELECT profiles_flatvalue.indicator_slug, profiles_flatvalue.display_title, profiles_flatvalue.geography_id, profiles_flatvalue.geography_name, profiles_flatvalue.geometry_id, profiles_flatvalue.value_type, profiles_flatvalue.time_key, profiles_flatvalue.number, profiles_flatvalue.percent, profiles_flatvalue.moe, profiles_flatvalue.numerator, profiles_flatvalue.numerator_moe, profiles_flatvalue.f_number, profiles_flatvalue.f_percent, profiles_flatvalue.f_moe, profiles_flatvalue.f_numerator, profiles_flatvalue.f_numerator_moe, ST_AsGeoJSON(maps_polygonmapfeature.geom) AS geom FROM profiles_flatvalue LEFT OUTER JOIN maps_polygonmapfeature ON (profiles_flatvalue.geography_geo_key = maps_polygonmapfeature.geo_key) WHERE profiles_flatvalue.indicator_slug = $1 AND profiles_flatvalue.time_key= $2"

	var query string

	// we need to support getting * geos or specific ones via thier ids also we need to be able to join on a geom
	if cleaned_geos == "*" {
		query = base_query

	} else {
		query = base_query + " AND profiles_flatvalue.geography_id IN (" + cleaned_geos + ")"
	}

	stmt, err := db.Prepare(query)
	if err != nil {
		log.Println("Error runnning query: getDataGeoJson")
		r := []byte("500")
		return r

	}
	defer stmt.Close()

	rows, err := stmt.Query(ind, cleaned_time)
	if err != nil {
		log.Println("Error runnning query: getDataGeoJson")
		r := []byte("500")
		return r
	}

	results := []interface{}{}
	metachan := make(chan []interface{}) // get the meta data
	go getMetaData(metachan, db, ind)
	data["related"] = <-metachan

	for rows.Next() {
		jrow := make(map[string]interface{})
		err := rows.Scan(&indicator_slug, &display_title, &geography_id, &geography_name, &geometry_id, &value_type, &time_key, &number, &percent, &moe, &numerator, &numerator_moe, &f_number, &f_percent, &f_moe, &f_numerator, &f_numerator_moe, &geom)
		if err == nil {
			if geom.Valid {
				jrow = jsonLoads(geom.String)
			} else {
				jrow["coordinates"] = nil
			}
			properties := make(map[string]interface{})
			properties["label"] = geography_name
			properties["geography_id"] = geography_id
			values := make(map[string]interface{})
			values["indicator_slug"] = indicator_slug
			values["value_type"] = value_type
			values["time_key"] = time_key
			if number.Valid {
				values["number"] = number.Float64
			} else {
				values["number"] = nil
			}
			if value_type != "i" {
				if percent.Valid {
					values["percent"] = percent.Float64
				} else {
					values["percent"] = nil
				}
			} else {
				values["percent"] = nil
			}
			if moe.Valid {
				values["moe"] = moe.Float64
			} else {
				values["moe"] = nil
			}

			if f_number.Valid {
				values["f_number"] = f_number.String

			} else {
				values["f_number"] = nil
			}

			if value_type != "i" {
				if numerator.Valid {
					values["numerator"] = numerator.Float64
				} else {
					values["numerator"] = nil
				}
				if f_numerator.Valid {
					values["f_numerator"] = f_numerator.String
				} else {
					values["f_numerator"] = nil
				}
				if numerator_moe.Valid {
					values["numerator_moe"] = numerator_moe.Float64
				} else {
					values["numerator_moe"] = nil
				}
				if f_numerator.Valid {
					values["f_numerator_moe"] = f_numerator_moe.String
				} else {
					values["f_numerator_moe"] = nil
				}
				if f_percent.Valid {
					values["f_percent"] = f_percent.String

				} else {
					values["f_percent"] = nil
				}
			} else {
				values["f_percent"] = nil
			}

			if f_moe.Valid {
				values["f_moe"] = f_moe.String

			} else {
				values["f_moe"] = nil
			}

			jrow["properties"] = &properties
			jrow["values"] = &values

			results = append(results, jrow)
		}
	}

	data["objects"] = &results
	j, err := json.Marshal(data)

	if len(results) != 0 {
		putInCache(conf.REDIS_CONN, hash, j, conf.CACHE_EXPIRE)
	}

	return j
}

func getGeomsByGeosId(geos_ids string, conf CONFIG) []byte {
	/*
	   geos_ids is a comma delimited string of IDS found in the profiles_georecord_table
	*/

	hash := cache.MakeHash("shp:" + geos_ids)
	c := getFromCache(conf.REDIS_CONN, hash)
	if len(c) != 0 {
		log.Println("Serving getData from shp cache:", geos_ids)
		return c
	}
	var (
		geos_id   int
		geos_name string
		geos_slug string
		geo_key   string
		geom      string
	)

	cleaned_geos, err := sanitize(geos_ids, "[0-9,\\*]+")

	if err != nil {
		r := []byte("405")
		return r
	}
	db, err := getDB(conf)
	if err != nil {
		log.Println("Error trying to call getDB")
		r := []byte("500")
		return r
	}
	defer db.Close()

	base_query := "SELECT profiles_georecord.geo_id, profiles_georecord.name, profiles_georecord.slug, profiles_georecord.geo_id, ST_ASGeoJSON(maps_polygonmapfeature.geom) as geom FROM profiles_georecord, maps_polygonmapfeature WHERE profiles_georecord.id IN(" + cleaned_geos + ") AND profiles_georecord.geo_id = maps_polygonmapfeature.geo_key"
	rows, err := db.Query(base_query)
	if err != nil {
		log.Println("Error runnning query: getGeomsByGeosId")
		r := []byte("500")
		return r
	}
	defer rows.Close()

	data := map[string]interface{}{} // this will be the object that wraps everything

	results := []interface{}{}
	for rows.Next() {
		err := rows.Scan(&geos_id, &geos_name, &geos_slug, &geo_key, &geom)
		if err == nil {
			properties := make(map[string]interface{})
			properties["geo_key"] = geo_key
			properties["label"] = geos_name
			properties["slug"] = geos_slug
			geom := jsonLoads(geom)
			geom["properties"] = &properties

			results = append(results, geom)
		}
	}
	data["objects"] = &results
	j, err := json.Marshal(data)

	if len(results) != 0 {
		putInCache(conf.REDIS_CONN, hash, j, conf.CACHE_EXPIRE)
	}

	return j

}

// Get Geoms by a list of geometry ids
func getGeomsById(geoms_ids string, conf CONFIG) []byte {
	/*
	   geoms_ids is a comma delimited string
	*/
	hash := cache.MakeHash("shp:" + geoms_ids)
	c := getFromCache(conf.REDIS_CONN, hash)
	if len(c) != 0 {
		log.Println("Serving getData from shp cache:", geoms_ids)
		return c
	}

	var (
		geom    string
		geo_key string
		label   string
	)

	cleaned_geos, err := sanitize(geoms_ids, "[0-9,\\*]+")

	if err != nil {
		r := []byte("405")
		return r
	}
	db, err := getDB(conf)
	if err != nil {
		log.Println("Error trying to call getDB")
		r := []byte("500")
		return r
	}
	defer db.Close()

	rows, err := db.Query("SELECT geo_key, label, ST_AsGeoJSON(geom) FROM maps_polygonmapfeature WHERE id IN (" + cleaned_geos + ")")

	if err != nil {
		log.Println("Error runnning query: getGeomsById")
		r := []byte("500")
		return r
	}
	defer rows.Close()

	data := map[string]interface{}{} // this will be the object that wraps everything
	results := []interface{}{}
	for rows.Next() {
		err := rows.Scan(&geo_key, &label, &geom)
		if err == nil {
			properties := make(map[string]interface{})
			properties["geo_key"] = geo_key
			properties["label"] = label
			geom := jsonLoads(geom)
			geom["properties"] = properties

			results = append(results, geom)
		}
	}
	data["objects"] = &results
	j, err := json.Marshal(data)

	if len(results) != 0 {
		putInCache(conf.REDIS_CONN, hash, j, conf.CACHE_EXPIRE)
	}

	return j
}

// Return a list of polygons that are IN or OF geom_id depending on what the geo_lev_id is
/* Example:
   IN query
   Find geoms of geolevel 6 that are in geom 419. lev=N or *
   http://127.0.0.1:8080/shp/q/?geoms=419&lev=6&q=IN

   OF query
   Find geoms of geolevel 1 that contain geoms 380
   http://127.0.0.1:8080/shp/q/?geoms=380&lev=1&q=OF

*/
func getGeoQuery(conf CONFIG, geoms_ids string, geo_lev_id string, query_type string) []byte {

	hash := cache.MakeHash("gGQ:" + geoms_ids + geo_lev_id + query_type)
	c := getFromCache(conf.REDIS_CONN, hash)
	if len(c) != 0 {
		log.Println("Serving getData from cache: ", "gGQ:"+geoms_ids+geo_lev_id+query_type)
		return c
	}

	cleaned_geoms, err := sanitize(geoms_ids, "[0-9,\\*]+")
	if err != nil {
		r := []byte("405")
		return r
	}

	cleaned_geo_lev, err := sanitize(geo_lev_id, "[0-9,]+") // supports mulitple levels
	if err != nil {
		r := []byte("405")
		return r
	}

	cleaned_query_type, err := sanitize(query_type, "(IN|OF)")
	if err != nil {
		r := []byte("405")
		return r
	}
	// also make sure the sanitized query_Type is IN or OF
	if cleaned_query_type != "IN" && cleaned_query_type != "OF" {
		r := []byte("405")
		return r
	}
	var (
		geomId int
		geosId int
		geoKey string
		label  string
		slug   string
	)
	var geom_query string
	where_clause := " WHERE profiles_georecord.level_id IN (" + cleaned_geo_lev + ") AND "

	if cleaned_query_type == "IN" {
		// find geoms contained in this geom
		geom_query = "WITH targ_levs AS (SELECT maps_polygonmapfeature.id as geom_id, maps_polygonmapfeature.geo_key, maps_polygonmapfeature.geom, profiles_georecord.id as id, profiles_georecord.slug, profiles_georecord.name as label FROM maps_polygonmapfeature, profiles_georecord" + where_clause + "maps_polygonmapfeature.geo_key=profiles_georecord.geo_id), targ_geom AS (SELECT id, geo_key, geom FROM maps_polygonmapfeature WHERE id IN (" + cleaned_geoms + ") LIMIT 1) SELECT DISTINCT ON (targ_levs.geo_key) targ_levs.id, targ_levs.geom_id, targ_levs.geo_key, targ_levs.label, targ_levs.slug FROM targ_levs, targ_geom WHERE ST_Contains(targ_geom.geom, ST_Centroid(targ_levs.geom)) ORDER BY targ_levs.geo_key"

	} else if cleaned_query_type == "OF" {
		// find geoms that contain geom
		geom_query = "WITH levs AS (SELECT maps_polygonmapfeature.id as geom_id, maps_polygonmapfeature.geo_key, maps_polygonmapfeature.geom, profiles_georecord.id as id, profiles_georecord.slug, profiles_georecord.name as label FROM maps_polygonmapfeature, profiles_georecord" + where_clause + "maps_polygonmapfeature.geo_key=profiles_georecord.geo_id), targ AS (SELECT id, geo_key, geom FROM maps_polygonmapfeature WHERE id IN (" + cleaned_geoms + ") LIMIT 1) SELECT DISTINCT ON (levs.geo_key) levs.id, levs.geom_id, levs.geo_key, levs.label, levs.slug FROM levs, targ WHERE ST_Contains(levs.geom, ST_Centroid(targ.geom)) ORDER BY levs.geo_key"

	}
	//TODO: We tend to always run querires like this, why not abstract it
	db, err := getDB(conf)
	if err != nil {
		log.Println("Error trying to call getDB")
		r := []byte("500")
		return r
	}
	defer db.Close()
	stmt, err := db.Prepare(geom_query)

	if err != nil {
		log.Println("Error preparing query: ", geom_query)
		r := []byte("405")
		return r
	}
	defer stmt.Close()

	rows, err := stmt.Query()

	if err != nil {
		log.Println("Error running query ", geom_query)
		r := []byte("405")
		return r
	}
	data := map[string]interface{}{} // this will be the object that wraps everything
	results := []interface{}{}
	for rows.Next() {
		jrow := make(map[string]interface{})
		err := rows.Scan(&geosId, &geomId, &geoKey, &label, &slug)
		if err == nil {
			jrow["id"] = geosId
			jrow["geom_id"] = geomId
			jrow["geoKey"] = geoKey
			jrow["label"] = label
			jrow["slug"] = slug
			results = append(results, jrow)
		} else {
			log.Println("Error in row: %s ", err)
		}
	}
	data["objects"] = &results
	j, err := json.Marshal(data)

	if len(results) != 0 {
		putInCache(conf.REDIS_CONN, hash, j, conf.CACHE_EXPIRE)
	}

	return j

}

// Return GeoRecords by Level slug
func getGeosByLevSlug(conf CONFIG, levslug string, filter_key string) []byte {

	hash := cache.MakeHash("gGByLS:" + levslug + filter_key)

	c := getFromCache(conf.REDIS_CONN, hash)
	if len(c) != 0 {
		log.Println("Serving getData from cache: ", "gGbyLS:"+levslug)
		return c
	}

	var (
		id     int
		geoKey string
		slug   string
		name   string
	)

	cleaned_slug, err := sanitize(levslug, "[-\\w]+")
	if err != nil {
		r := []byte("405")
		return r
	}

	cleaned_filter, err := sanitize(filter_key, "[-\\w\\d]+")
	if err != nil {
		//r:=[]byte("405")
		//return r
		cleaned_filter = ""
	}

	db, err := getDB(conf)
	if err != nil {
		log.Println("Error trying to call getDB")
		r := []byte("500")
		return r
	}
	defer db.Close()

	query := "SELECT profiles_georecord.id, profiles_georecord.geo_id, profiles_georecord.slug, profiles_georecord.name FROM profiles_geolevel FULL JOIN profiles_georecord ON profiles_georecord.level_id = profiles_geolevel.id WHERE profiles_geolevel.slug=$1"

	if cleaned_filter != "" {
		query += " AND profiles_georecord.geo_id ILIKE '%" + cleaned_filter + "%'"
	}

	rows, err := db.Query(query, cleaned_slug)
	if err != nil {
		log.Println("Error runnning query: %s", query)
		r := []byte("500")
		return r
	}
	defer rows.Close()

	data := map[string]interface{}{} // this will be the object that wraps everything
	results := []interface{}{}
	for rows.Next() {
		err := rows.Scan(&id, &geoKey, &slug, &name)
		if err == nil {
			jrow := make(map[string]interface{})
			jrow["id"] = id
			jrow["geoKey"] = geoKey
			jrow["slug"] = slug
			jrow["name"] = name
			results = append(results, jrow)
		}
	}

	data["objects"] = &results
	j, err := json.Marshal(data)

	if len(results) != 0 {
		putInCache(conf.REDIS_CONN, hash, j, conf.CACHE_EXPIRE)
	}

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
	for i := 0; i < len(st); i++ {
		val, err := strconv.Atoi(st[i])
		if err == nil {
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

func getDB(conf CONFIG) (*sql.DB, error) {
	conn_str := fmt.Sprintf("user=%s dbname=%s host=%s port=%s password=%s  sslmode=disable",
		conf.DB_USER, conf.DB_NAME, conf.DB_HOST, conf.DB_PORT, conf.DB_PASS)
	db, err := sql.Open("postgres", conn_str)
	err = db.Ping()
    if err != nil{
        log.Print(err, conn_str)
    }
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

	m := martini.Classic()

	m.Get("/csv/", func(res http.ResponseWriter, req *http.Request) {
		res.Header().Set("Content-Disposition", "attachment;filename=profilesdata.csv")
		res.Header().Set("Access-Control-Allow-Origin", "*")
		inds := req.FormValue("i")
		geos := req.FormValue("g")
		getDataCSV(res, inds, geos, conf) // the response codes and data are written in the handler func
	})

	m.Get("/indicator/:slug", func(res http.ResponseWriter, req *http.Request, params martini.Params) (int, string) {
		res.Header().Set("Content-Type", "application/json")
		res.Header().Set("Access-Control-Allow-Origin", "*")
		ind := params["slug"]
		time := req.FormValue("time")
		raw_geos := req.FormValue("geos")
		getGeom := req.FormValue("geom") // should we get the geos?
		var r []byte
		if getGeom == "" {
			r = getData(ind, time, raw_geos, conf)
			r := string(r[:])
			if r != "405" {
				return 200, r
			} else {
				return 405, "time and geos is Required"
			}
		} else {
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

	m.Get("/shp/", func(res http.ResponseWriter, req *http.Request) (int, string) {
		res.Header().Set("Content-Type", "application/json")
		res.Header().Set("Access-Control-Allow-Origin", "*")
		var r []byte
		geos := req.FormValue("geos")
		r = getGeomsByGeosId(geos, conf)
		rs := string(r[:])
		if rs == "405" {
			return 405, "geos Parameter is Required"
		} else if rs == "500" {
			return 500, "Server Error"
		} else {
			return 200, rs
		}

	})

	m.Get("/shp/q/", func(res http.ResponseWriter, req *http.Request) (int, string) {
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

	m.Get("/geos/level/:slug", func(res http.ResponseWriter, req *http.Request, params martini.Params) (int, string) {
		res.Header().Set("Content-Type", "application/json")
		res.Header().Set("Access-Control-Allow-Origin", "*")
		filter := req.FormValue("filter") // a string to fuzzy match geokeys on
		var r []byte
		r = getGeosByLevSlug(conf, params["slug"], filter)
		rs := string(r[:])
		if rs == "405" {
			return 405, "valid slug is required"
		} else if rs == "500" {
			return 500, "Server Error"
		} else {
			return 200, rs
		}

	})

	http.ListenAndServe(settings_port, m)
}
