curl -u "user:password" -X POST -H "Content-Type: application/json" -d '{
  "aggs": {
    "xxx": {
      "filter": {
        "term": {
          "hasFeatureCollection.place_postalAddress_feature.featureValue": "francisco"
        }
      },
      "aggs": {
        "city": {
          "terms": {
            "field": "hasFeatureCollection.place_postalAddress_feature.featureObject.addressLocality",
            "size": 500
          },
          "aggs": {
            "top_tag_hits": {
              "top_hits": {
                "_source": {
                  "include": [
                    "hasFeatureCollection.place_postalAddress_feature.featureObject.addressLocality",
                    "hasFeatureCollection.place_postalAddress_feature.featureObject.addressRegion",
                    "hasFeatureCollection.place_postalAddress_feature.featureObject.addressCountry"
                  ]
                },
                "size": 1
              }
            }
          }
        }
      }
    }
  },
  "size": 0
}' "https://esc.memexproxy.com/dig-latest/WebPage/_search"