from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from bson.decimal128 import Decimal128
from typing import Any, Dict

app = FastAPI()

# MongoDB client setup
client = MongoClient("localhost", 27017)
db = client['test']
collection = db['sample_airbnb_listingsAndReview']

def convert_decimal128_to_float(data: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively converts Decimal128 objects to floats in a dictionary."""
    for key, value in data.items():
        if isinstance(value, Decimal128):
            data[key] = float(value.to_decimal())
        elif isinstance(value, dict):
            data[key] = convert_decimal128_to_float(value)
        elif isinstance(value, list):
            data[key] = [convert_decimal128_to_float(item) if isinstance(item, dict) else item for item in value]
    return data
@app.get("/")
def welcome():
    return {"message": "Welcome to BnB Listing"}
#top10 city listing based on rating
@app.get("/listings/top10/city/{city}")
def get_top10_listings_for_city(city: str, skip: int = 0, limit: int = 10):
    try:
        # MongoDB fetch operation with pagination
        listings_cursor = collection.find(
            {"address.market": city},
            {"name": 1 , "listing_url": 1 , "summary":1 , "address.market" : 1, "review_scores.review_scores_rating":1}
        ).sort("review_scores.review_scores_rating", -1).skip(skip).limit(limit)
        
        listings = list(listings_cursor)
        
        # Convert Decimal128 objects to float
        listings = [convert_decimal128_to_float(listing) for listing in listings]
        
        return {"Top 10 in the city based on ratings": listings}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    

@app.get("/cities/top5/average_rating")
def get_top5_cities_with_highest_avg_rating():
    try:
        # MongoDB aggregation to calculate the top 5 cities with the highest average rating score
        pipeline = [
            {
                "$group": {
                
                    "_id": "$address.market",
                    "average_rating": {"$avg": "$review_scores.review_scores_rating"},
                    "Listings" : {"$push":{
                    "name" : "$name",
                    "Type" : "$property_type",
                    "Room Type" : "$room_type",
                    "Price" : "$price",
                                       
                    }}
                    
                }
            },
            {"$sort": {"average_rating": -1}},
            {"$limit": 5},
            {"$project" : {"city":"$_id" , "average_rating" : 1, "Listings" : 1}}
        ]
        
        cities = list(collection.aggregate(pipeline))
        
        # Convert Decimal128 objects to float in the aggregation result
        cities = [convert_decimal128_to_float(city) for city in cities]
        
        return {"Top 5 cities with highest average rating": cities}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@app.get("/listings/country/{country}")
def get_listings_for_country(country: str):
    try:
        # MongoDB fetch operation to find all listings for the given country
        listings_cursor = collection.find(
            {"address.country": country},
            {"_id": 0, "name": 1, "summary": 1, "address.street": 1, "address.suburb": 1, "property_type": 1, "review_scores.review_scores_rating": 1, "price": 1, "cleaning_fee": 1}
        ).sort("review_scores.review_scores_rating", -1)
        
        
        listings = list(listings_cursor)
        
        # Convert Decimal128 objects to float
        listings = [convert_decimal128_to_float(listing) for listing in listings]
        
        return {"Listings in the country": listings}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    
@app.get("/listings/top10/property_type/{property_type}")
def get_top10_listings_for_property_type(property_type: str, skip: int = 0, limit: int = 10):
     try:
        # MongoDB fetch operation with pagination
        listings_cursor = collection.find(
            {"property_type": property_type},
            {"_id": 0, "name": 1, "summary": 1, "address.country": 1, "address.suburb": 1, "property_type": 1, "review_scores.review_scores_rating": 1, "price": 1, "cleaning_fee": 1}
        ).sort("review_scores.review_scores_rating", -1).skip(skip).limit(limit)
        
        listings = list(listings_cursor)
        
        # Convert Decimal128 objects to float
        listings = [convert_decimal128_to_float(listing) for listing in listings]
        
        return {"Top 10 listings for property type": listings}
     except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@app.get("/listing/{country}/{property_type}/less than 300")
def get_listing_price300 (country: str, property_type: str):
    try:
        listings_cursor = collection.find(
            {"address.country": country, "property_type":property_type, "price":{"$lt":300}},{"_id": 0, "name":1,"address.market":1,"address.suburb":1,"beds":1,"price":1,"cleaning_fee":1, "review_scores.review_scores_rating":1 }
        ).sort([("price",-1), ("review_scores.review_scores.rating",1)])
        listings = list(listings_cursor)
        listings = [convert_decimal128_to_float(listing) for listing in listings]
        return{"Listings in the country under $300":listings}
    except Exception as e:
        raise HTTPException(status_code=500,detail=f"Internal Server Error:{str(e)}")

@app.get("/listing/{country}/{property_type}/with-more-than-10-amenities")
def get_listing_10amenities(country: str, property_type: str):
    try:
        pipeline = [
            {
                "$match": {
                    "address.country": country,
                    "property_type": property_type,
                    "$expr": {
                        "$gt": [{"$size": "$amenities"}, 10]
                    }
                }
            },
            {
                "$addFields": {
                    "total_price_include_cleaning_fee": {
                        "$add": [
                            {"$ifNull": ["$price", 0]},
                            {"$ifNull": ["$cleaning_fee", 0]}
                        ]
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "name": 1,
                    "address.market": 1,
                    "address.suburb": 1,
                    "beds": 1,
                    "review_scores.review_scores_rating": 1,
                    "total_price_include_cleaning_fee": 1,
                    "amenities": 1 ,
                }
            },
            {
                "$sort": {
                    "total_price_include_cleaning_fee": 1
                }
            },
            {
                "$limit": 5
            }
        ]
        
        listings = list(collection.aggregate(pipeline))
        
        # Convert Decimal128 objects to float
        listings = [convert_decimal128_to_float(listing) for listing in listings]
        
        return {"Top 5 listings with more than 10 amenities": listings}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@app.get("/hotel/{country}/under $500")
def get_hotel_under_500(country: str):
    try:
        pipeline = [
            {
                "$match": {
                    "address.country": country,
                    "property_type": "Hotel",
                    "price": {"$lt": 500}
                }
            },
            {
                "$sort": {"price": 1}  # Optional: Sort by price in ascending order
            },
            {
                "$limit": 1
            },
            {
                "$project": {
                    "_id": 0,
                    "name": 1,
                    "address": 1,
                    "price": 1,
                    "review_scores": 1,
                    "amenities": 1
                }
            }
        ]
        
        hotel = list(collection.aggregate(pipeline))
        
        if not hotel:
            raise HTTPException(status_code=404, detail="No hotel found under 500 in the given country.")
        
        # Convert Decimal128 objects to float
        hotel = [convert_decimal128_to_float(h) for h in hotel]
        
        return {"Hotel under $500 in the country": hotel[0]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@app.get("/listings/{country}/{property_type}/half-price")
def get_listings_with_half_price(country: str, property_type: str):
    try:
        pipeline = [
            {
                "$match": {
                    "address.country": country,
                    "property_type": property_type
                }
            },
            {
                "$addFields": {
                    "half_price": {
                        "$divide": ["$price", 2]
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "name": 1,
                    "address.market": 1,
                    "price": 1,
                    "half_price": 1,
                    
                }
            }
        ]
        
        listings = list(collection.aggregate(pipeline))
        
        # Convert Decimal128 objects to float
        listings = [convert_decimal128_to_float(listing) for listing in listings]
        
        return {"Listings with half price": listings}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    
@app.get("/listings/{country}/at-most-5-max-guest")
def get_listings_with_at_most_5_max_guest(country: str):
    try:
        pipeline = [
            {
                "$match": {
                    "address.country": country,
                    "guests_included": {"$lte": 5}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "name": 1,
                    "price": 1,
                    "minimum_nights": 1,
                    "property_type": 1,
                    "guests_included": 1,
                    "address.suburb":1,
                    "address.market":1
                }
            },
            {
                 "$sort": {"price": 1} 
                
            }
        ]
        
        listings = list(collection.aggregate(pipeline))
        
        # Convert Decimal128 objects to float
        listings = [convert_decimal128_to_float(listing) for listing in listings]
        
        return {"Listings with at most 5 maximum guest included in the staycation": listings}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@app.get("/apartments/{country}/top15-with-responsive-host")
def get_top15_apartments(country: str):
    try:
        pipeline = [
            {
                "$match": {
                    "address.country": country,
                    "property_type": "Apartment",
                    "host.host_response_rate": {"$gte": 90},
                    "host.host_identity_verified": True
                }
            },
            {
                "$addFields": {
                    "average_rating": {"$avg": "$review_scores.review_scores_rating"}
                }
            },
            {
                "$sort": {
                    "average_rating": -1
                }
            },
            {
                "$limit": 15
            },
            {
                "$project": {
                    "_id": 0,
                    "name": 1,
                    "address.suburb": 1,
                    "price": 1,
                    "average_rating": 1,
                    "host": {
                        "host_name": 1,
                        "host_response_rate": 1,
                        "host_identity_verified": 1
                    }
                }
            }
        ]
        
        apartments = list(collection.aggregate(pipeline))
        
        # Convert Decimal128 objects to float
        apartments = [convert_decimal128_to_float(apartment) for apartment in apartments]
        
        return {"Top 15 apartments in the country with only verified host and at least 90% responsive rate": apartments}
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
