package util

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
        "log"
)

func Post() {

	url := "http://xxxxx:8080/v2/repos/wh_flowDataSource1/data"

	payload := strings.NewReader("a=111")

	req, _ := http.NewRequest("POST", url, payload)

	req.Header.Add("Date", "Tue, 11 Sep 2018 10:57:09 GMT")
	req.Header.Add("Authorization", "oqSBNbmgAAGI155F6MJ3N2Tk9ruL_6XQpx-uxkkg:8bUg3Iy5CVzU3vXyJyZXNvdXJjZSI6Ii92Mi9yZXBvcy93aF9mbG93RGF0YVNvdXJjZTEvZGF0YSIsImV4cGlyZXMiOjE1MzY2OTkwODgsIIiLCJjb250ZW50VHlwZSI6InRleHQvcGxhaW4iLCJoZWFkZXJzIjoiIiwibWV0aG9kIjoiUE9TVCJ9")
	req.Header.Add("Content-Type", "text/plain")

	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()
	body, _ := ioutil.ReadAll(res.Body)

	fmt.Println(res)
	fmt.Println(string(body))

}

func Get() {

	url := "http://xxxxx:8080/v2/repos/wh_flowDataSource1"

	req, _ := http.NewRequest("GET", url, nil)

	req.Header.Add("Authorization", "BNbmgAAGI155F6MJ3N2Tk9ruL_6XQpx-uxkkg:tGCY3xCsgybHd5IjcDMi9yZXBvcy93aF9mbG93RGF0YVNvdXJjZTEiLCJleHBpcmVzIjoxNTM2NzU4NjQ3LCJjb250ZW5VudFR5cGUiOiIiLCJoZWFkZXJzIjoiIiwibWV0aG9kIjoiR0VUIn0=")

	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()
	body, _ := ioutil.ReadAll(res.Body)

	fmt.Println(res)
	fmt.Println(string(body))

}

func Put() {

	url := "http://xxxxx:8080/v2/repos/wh_flowDataSource1"

	payload := strings.NewReader("{\n    \"schema\": [\n      {\n        \"key\": \"a\",\n        \"valtype\": \"string\",\n        \"required\": false\n      }\n    ]\n}")

	req, _ := http.NewRequest("PUT", url, payload)

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "bmgAAGI155F6MJ3N2Tk9ruL_6XQpx-uxkkg:yKx_OYDtI3njD7-c7Y87Oov0GpI=:eyJyZXNvdXJBvcy93aF9mbG93RGF0YVNvdXJjZTEiLCJleHBpcmVzIjoxNTM2NzU1MjkwLCJjb250ZW50TUQ1IjoiIiwiY29udGVudFR5cGUiOiJhcHBsaWNhdGlvbi9qc29uIiwiaGVhZGVycyI6IiIsIm1ldGhvZCI6IlBVVCJ9")
	req.Header.Add("Date", "Wed, 12 Sep 2018 02:10:09 GMT")

	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()
	body, _ := ioutil.ReadAll(res.Body)

	fmt.Println(res)
	fmt.Println(string(body))
}



func Delete(job string,instance string) {
	url := "http://172.18.18.99:9091/metrics/job/"+job+"/instance/"+instance
        log.Println(url)
	req, _ := http.NewRequest("DELETE", url, nil)
	res, _ := http.DefaultClient.Do(req)
	defer res.Body.Close()
	body, _ := ioutil.ReadAll(res.Body)
	fmt.Println(res)
	fmt.Println(string(body))
}

