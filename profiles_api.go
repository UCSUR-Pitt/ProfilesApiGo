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
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"./cache"
	"github.com/go-martini/martini"
	_ "github.com/lib/pq"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
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

// TODO: this stuct needs to be implemented everywhere we fetch FlatValue data.
type FlatValue struct {
	Display_title    string
	Geography_level  string
	Geography_name   string
	Geography_geokey string
	Time_key         string
    Value_type       string
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
        if f.Value_type == "i"{
		    m["number"] = f.F_number.String
        }else{

		    m["number"] = "n/a"
        }
	}
	if !f.Percent.Valid {
		m["percent"] = ""
	} else {
        if f.F_percent.String == "0.0%"{
		    m["percent"] = ""
        }else{
		    m["percent"] = f.F_percent.String
        }
	}

	if !f.Moe.Valid {
		m["moe"] = ""
        m["pct_moe"] = ""
	} else {
        if f.Value_type != "i"{
            // this is a denominator and we moe should be a percentage.
            m["moe"] = "n/a"
            m["pct_moe"] = fmt.Sprintf("%v%%", f.Moe.Float64)
            // we need to n/a estimates
            m["number"] = "n/a"
        }else{
            // regular indicator moe
		    m["moe"] = fmt.Sprintf("%v", f.Moe.Float64)
            m["pct_moe"] = "n/a"
        }
	}

	return m
}

func toSlice(f FlatValue) []string { // TODO: why cant I make this part of the struct. Error is: Cannot call pointer method on val.
	s := []string{}
	fM := f.ToMap()
	// append fields in order, we cant simply loop over the keys.
	s = append(s, []string{fM["number"], fM["moe"], fM["percent"], fM["pct_moe"]}...)
	return s
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
		geography_geo_key  string
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

	data := map[string]interface{}{} // this will be the object that wraps everything

	db, err := getDB(conf)
	if err != nil {
		log.Println("Error trying to call getDB")
		r := []byte("500")
		return r
	}
	defer db.Close()

	var base_query string = "SELECT indicator_slug, display_title, geography_id, geography_geo_key, geography_name, geometry_id, value_type, time_key, number, percent, moe, numerator,numerator_moe, f_number, f_percent, f_moe, f_numerator, f_numerator_moe FROM profiles_flatvalue WHERE indicator_slug = $1 AND time_key= $2"
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

	rows, err := stmt.Query(ind, time)
	if err != nil {
		log.Println("Error running query %s in getData", query)
	}

	results := []interface{}{}
	metachan := make(chan []interface{}) // get the meta data
	go getMetaData(metachan, db, ind)
	data["related"] = <-metachan

	for rows.Next() {
		jrow := make(map[string]interface{})
		err := rows.Scan(&indicator_slug, &display_title, &geography_id, &geography_geo_key, &geography_name, &geometry_id, &value_type, &time_key, &number, &percent, &moe, &numerator, &numerator_moe, &f_number, &f_percent, &f_moe, &f_numerator, &f_numerator_moe)
		if err != nil {
			log.Fatal(err)
		}
		jrow["indicator_slug"] = indicator_slug
		jrow["display_title"] = display_title
		jrow["geography_id"] = geography_id
		jrow["geography_geo_key"] = geography_geo_key
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
		log.Println("Error trying to call getDB: GetDataCSV")
		http.Error(res, "", 500)
		return
	}
	defer db.Close()

	var time string
	var timeSet []string
	var flatValues = make(map[string]map[string]map[string]FlatValue) //{indid:{geoid:{time1, time2}}}
	var flatValKeys = []string{}

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
	query := "WITH T1 AS (SELECT indicator_id, display_title, geography_id, geography_name, geography_geo_key, time_key, value_type, number, percent, moe, f_number, f_percent, f_moe FROM profiles_flatvalue WHERE indicator_id IN (%v) AND geography_id IN(%v) AND time_key != 'change'), T2 AS (SELECT DISTINCT ON (indicators_id) * FROM profiles_groupindex WHERE indicators_id IN (%v)), T3 AS (SELECT T1.*, T2.* FROM T1 LEFT OUTER JOIN T2 ON T1.indicator_id=T2.indicators_id), T4 AS ( SELECT profiles_georecord.id AS geography_id, profiles_geolevel.id AS level_id, profiles_geolevel.name AS geography_level FROM profiles_georecord LEFT OUTER JOIN profiles_geolevel ON profiles_geolevel.id = profiles_georecord.level_id ) SELECT display_title, geography_level, geography_name, geography_geo_key, time_key, value_type, number, percent, moe, f_number, f_percent, f_moe FROM T3 LEFT JOIN T4 ON T3.geography_id = T4.geography_id ORDER BY \"order\""

	query = fmt.Sprintf(query, cleaned_inds, cleaned_geos, cleaned_inds)

	stmt, err := db.Prepare(query)
	if err != nil {
		log.Println("Error Preparing query: getDataCSV")
		log.Println(err)
		http.Error(res, "", 500)
		return

	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		log.Fatal(err)
	}
	geoDetails := map[string]map[string]string{}

	for rows.Next() {
		v := FlatValue{}
		err := rows.Scan(&v.Display_title, &v.Geography_level, &v.Geography_name, &v.Geography_geokey, &v.Time_key, &v.Value_type, &v.Number, &v.Percent, &v.Moe, &v.F_number, &v.F_percent, &v.F_moe)

		if err == nil {
			// collect the the geokey and name (TODO: There's probably a better way to get these)
			geoDetails[v.Geography_name] = make(map[string]string)
			geoDetails[v.Geography_name]["key"] = v.Geography_geokey
			geoDetails[v.Geography_name]["lvl"]	= v.Geography_level

			// add a new key to our map for each indicator if it doesnt exists
			_, exists := flatValues[v.Display_title]
			
			if !exists {
				//-------------------------------------geoid: {timekey: FV}-----------------------------------//
				flatValues[v.Display_title] = make(map[string]map[string]FlatValue)
				flatValKeys = append(flatValKeys, v.Display_title)
			}
			// Now we add the geography key
			_, exists = flatValues[v.Display_title][v.Geography_name]

			if !exists {
				// it doesnt exist so we are gonna create a key for the new geokey
				flatValues[v.Display_title][v.Geography_name] = make(map[string]FlatValue)
				// now add placeholders for all the times.
				for _, t := range timeSet {
					flatValues[v.Display_title][v.Geography_name][t] = FlatValue{Display_title: v.Display_title, Time_key: t}
				}
			}

			// actually store the data
			flatValues[v.Display_title][v.Geography_name][v.Time_key] = v
		}
	}

	csvWriter := csv.NewWriter(res)
	header := []string{"indicator", "geography_level", "geography_name", "geography_id"}
	// generate a header
	for _, tVal := range timeSet {
		header = append(header, []string{tVal + "_est", tVal + "_moe", tVal + "_pct", tVal + "_pct_moe"}...)
	}

	csvWriter.Write(header)
	csvWriter.Flush()

	// at this point our values are prepped for export
	for _, indKey:= range flatValKeys {
				
		// now iterate Geos
		sortedGeoKeys := sortGeoKeys(flatValues[indKey])
       
		for _, sgk := range sortedGeoKeys {

			csvRow := []string{indKey}			
			csvRow = append(csvRow, []string{geoDetails[sgk]["lvl"], sgk,  geoDetails[sgk]["key"]}...)

			// now iterate time vals
			indGeo := flatValues[indKey][sgk]
			for _, time := range timeSet {
				csvRow = append(csvRow, toSlice(indGeo[time])...)
			}
			csvWriter.Write(csvRow)
			csvWriter.Flush()
		}
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
		geography_geo_key string
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

	data := map[string]interface{}{} // this will be the object that wraps everything

	db, err := getDB(conf)
	if err != nil {
		log.Println("Error trying to call getDB")
		r := []byte("500")
		return r
	}
	defer db.Close()
	var base_query = "SELECT profiles_flatvalue.indicator_slug, profiles_flatvalue.display_title, profiles_flatvalue.geography_id, profiles_flatvalue.geography_geo_key, profiles_flatvalue.geography_name, profiles_flatvalue.geometry_id, profiles_flatvalue.value_type, profiles_flatvalue.time_key, profiles_flatvalue.number, profiles_flatvalue.percent, profiles_flatvalue.moe, profiles_flatvalue.numerator, profiles_flatvalue.numerator_moe, profiles_flatvalue.f_number, profiles_flatvalue.f_percent, profiles_flatvalue.f_moe, profiles_flatvalue.f_numerator, profiles_flatvalue.f_numerator_moe, ST_AsGeoJSON(maps_polygonmapfeature.geom) AS geom FROM profiles_flatvalue LEFT OUTER JOIN maps_polygonmapfeature ON (profiles_flatvalue.geography_geo_key = maps_polygonmapfeature.geo_key) WHERE profiles_flatvalue.indicator_slug = $1 AND profiles_flatvalue.time_key= $2"

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

	rows, err := stmt.Query(ind, time)
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
		err := rows.Scan(&indicator_slug, &display_title, &geography_id, &geography_geo_key, &geography_name, &geometry_id, &value_type, &time_key, &number, &percent, &moe, &numerator, &numerator_moe, &f_number, &f_percent, &f_moe, &f_numerator, &f_numerator_moe, &geom)
		if err == nil {
			if geom.Valid {
				jrow = jsonLoads(geom.String)
			} else {
				jrow["coordinates"] = nil
			}
			properties := make(map[string]interface{})
			properties["label"] = geography_name
			properties["geography_geo_key"] = geography_geo_key
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
                geom_query = "WITH targ_levs AS (SELECT maps_polygonmapfeature.id as geom_id, maps_polygonmapfeature.geo_key, ST_SimplifyPreserveTopology(maps_polygonmapfeature.geom, 0.001) AS geom, profiles_georecord.id as id, profiles_georecord.slug, profiles_georecord.name as label FROM maps_polygonmapfeature, profiles_georecord" + where_clause + "maps_polygonmapfeature.geo_key=profiles_georecord.geo_id), targ_geom AS (SELECT id, geo_key, ST_SimplifyPreserveTopology(geom, 0.001) AS geom FROM maps_polygonmapfeature WHERE id IN (" + cleaned_geoms + ") LIMIT 1) SELECT DISTINCT ON (targ_levs.geo_key) targ_levs.id, targ_levs.geom_id, targ_levs.geo_key, targ_levs.label, targ_levs.slug FROM targ_levs, targ_geom WHERE ST_Area(ST_Intersection(targ_levs.geom, targ_geom.geom)) > ST_Area(targ_levs.geom)/2 ORDER BY targ_levs.geo_key"
	} else if cleaned_query_type == "OF" {
		// find geoms that contain geom
                geom_query = "WITH targ_levs AS (SELECT maps_polygonmapfeature.id as geom_id, maps_polygonmapfeature.geo_key, ST_SimplifyPreserveTopology(maps_polygonmapfeature.geom, 0.001) AS geom, profiles_georecord.id as id, profiles_georecord.slug, profiles_georecord.name as label FROM maps_polygonmapfeature, profiles_georecord" + where_clause + "maps_polygonmapfeature.geo_key=profiles_georecord.geo_id), targ_geom AS (SELECT id, geo_key, ST_SimplifyPreserveTopology(geom, 0.001) AS geom FROM maps_polygonmapfeature WHERE id IN (" + cleaned_geoms + ") LIMIT 1) SELECT DISTINCT ON (targ_levs.geo_key) targ_levs.id, targ_levs.geom_id, targ_levs.geo_key, targ_levs.label, targ_levs.slug FROM targ_levs, targ_geom WHERE ST_Area(ST_Intersection(targ_levs.geom, targ_geom.geom)) > ST_Area(targ_geom.geom)/2 ORDER BY targ_levs.geo_key"
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
		log.Println(err)
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

func getPointOverlays(conf CONFIG) []byte {
	/*
	   get all enabled PointOverlay (point shapefile overlays)
	*/

	hash := cache.MakeHash("maplayers")
	c := getFromCache(conf.REDIS_CONN, hash)
	if len(c) != 0 {
		log.Println("Serving getPointOverlays from cache")
		return c
	}

	db, err := getDB(conf)
	if err != nil {
		log.Println("Error trying to call getDB")
		r := []byte("500")
		return r
	}
	defer db.Close()

	var (
		name   string
                shapefile_id string
                image string
		label   string
		geom      string
	)

	query := "SELECT maps_pointoverlay.name, maps_pointoverlay.shapefile_id, maps_pointoverlayicon.image FROM maps_pointoverlay, maps_pointoverlayicon WHERE maps_pointoverlayicon.id = maps_pointoverlay.icon_id AND maps_pointoverlay.available_on_maps = TRUE"

	rows, err := db.Query(query)
	if err != nil {
		log.Println("Error runnning query: getPointOverlays")
		r := []byte("500")
		return r
	}
	defer rows.Close()

	data := map[string]interface{}{}
	for rows.Next() {
		err := rows.Scan(&name, &shapefile_id, &image)
		if err == nil {

			rows, err := db.Query("SELECT label, ST_ASGeoJSON(geom) AS geom FROM maps_pointmapfeature WHERE source_id = " + shapefile_id)
			if err != nil {
				log.Println("Error runnning query: getPointOverlays")
				r := []byte("500")
				return r
			}
			defer rows.Close()

			results := []interface{}{}
			for rows.Next() {
				err := rows.Scan(&label, &geom)
				if err == nil {
					properties := make(map[string]interface{})
					properties["label"] = label
					properties["image"] = "/media/" + image
					geom := jsonLoads(geom)
					geom["properties"] = &properties
					results = append(results, geom)
				}
			}
			data[name] = results
		}
	}

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
	if err != nil {
		log.Print(err, conn_str)
	}
	return db, err
}

// return a sorted slice of keys 
func sortGeoKeys(m map[string]map[string]FlatValue) []string { 
    su := map[string]string{}
    s := []string{}
    for key, _ := range(m) {
        su[key]=key
    }
    for key,_ := range(su){
        s = append(s, key)
    }

    sort.Strings(s)
    return s
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

	m.Get("/csv/", func(res http.ResponseWriter, req *http.Request){
        	//TODO: Why does this respose comeback as "Canceled"
		inds := req.FormValue("i")
		geos := req.FormValue("g")
        	domain_name := req.FormValue("dom")
        	if(domain_name == ""){
            		domain_name ="profiles"
        	}

		res.Header().Set("Content-Disposition", fmt.Sprintf("attachment;filename=%s.csv", strings.Replace(domain_name, ",", " ", -1)))
		res.Header().Set("Access-Control-Allow-Origin", "*")
        	res.Header().Set("Status","200")
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

	m.Get("/point-overlays/", func(res http.ResponseWriter, req *http.Request) (int, string) {
		res.Header().Set("Content-Type", "application/json")
		res.Header().Set("Access-Control-Allow-Origin", "*")
		var r []byte
		r = getPointOverlays(conf)
		rs := string(r[:])
		if rs == "500" {
			return 500, "Server Error"
		} else {
			return 200, rs
		}

	})

	http.ListenAndServe(settings_port, m)
}
