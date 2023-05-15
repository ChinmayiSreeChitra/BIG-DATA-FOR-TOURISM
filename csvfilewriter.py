from yelpapi import YelpAPI
import random
from kafka import KafkaProducer, KafkaConsumer
from hdfs import InsecureClient
import csv
import json
hdfs_client = InsecureClient('http://localhost:9870')
# Replace 'YOUR_API_KEY_HERE' with your actual Yelp API key
api_key = 'ipnm5Zmljo6gV9kdduLc_p5OWr6G01iUdKEoUop3kYPb7odjrWlC08iCQEhPSlZcWFdSWE4D5Q6NsbbOkLbSZqHuMcpz7i3ew25hH9f74Yr2LjPjf_wtsdlTnAJTZHYx'

yelp_api = YelpAPI(api_key)

location = input("Enter the city or state you want to search for tourist attractions: ")
days = int(input("Enter the number of days you will be visiting: "))

# Radius for search query, in meters
radius = 20000

# Maximum number of attractions to search for
max_attractions = 50

# Minimum rating for an attraction to be considered
min_rating = 4.0

# List of attractions for each day
itinerary = [[] for _ in range(days)]

for i in range(days):
    while len(itinerary[i]) < 3:
        # Search for attractions within the given radius
        response = yelp_api.search_query(term='tourist attractions', location=location, sort_by='rating',
                                         limit=max_attractions, radius=radius)

        # Filter attractions based on minimum rating
        attractions = [business for business in response['businesses'] if business['rating'] >= min_rating]

        if not attractions:
            # If there are no attractions matching criteria, increase rating threshold and try again
            min_rating -= 0.1
            continue

        # Randomly select an attraction for the day
        attraction = random.choice(attractions)

        # Add attraction to itinerary
        itinerary[i].append((attraction['name'], attraction['location']['address1']))

    # Shuffle the attractions for the day
    random.shuffle(itinerary[i])

# Print the itinerary

with open(f"itinerary_{location.replace(' ', '_')}.txt", 'w') as f, open(f"itinerary_{location.replace(' ', '_')}.csv", 'w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(['Day', 'Attraction', 'Location'])

    for i in range(days):
        f.write(f"Day {i + 1}:\n")
        print(f"Day {i + 1}:")
        messages_sent = 0
        for j, attraction in enumerate(itinerary[i]):
            f.write(f"- {attraction[0]} ({attraction[1]})\n")
            print(f"- {attraction[0]} ({attraction[1]})")
            writer.writerow([f"Day {i + 1}", attraction[0], attraction[1]])

            # Create a Kafka producer and send the attraction data as a JSON-encoded message to the Kafka topic
            producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                     value_serializer=lambda x: json.dumps(x).encode('utf-8'))
            producer.send('attractions', value=attraction)
            messages_sent += 1

        # Break out of the loop after sending all the messages for the day
        if i == days - 1:
            break

# Create a list to hold the attraction data
attractions = []

# Consume messages from the Kafka topic and add them to the attractions list
consumer = KafkaConsumer('attractions', bootstrap_servers=['localhost:9092'], value_deserializer=json.loads)
for message in consumer:
    attractions.append(message.value)
    if len(attractions) == days * 3:
        break
# Print the attraction list
print(attractions)

