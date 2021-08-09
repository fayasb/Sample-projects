import math

# This is a program written to calculate the efficiency and optimum stop for
# a hyperloop. This program is specific for a certain text input.



def read_input_file(file_name):
    with open(file_name) as f:
        lines = f.readlines()
    formatted_lines = [i.replace('\n', '') for i in lines]
    return formatted_lines

def getStops(all_stops, no_of_locations):
    stops = []
    for i in range(1, no_of_locations + 1):
        all_stops[i] = all_stops[i].replace('\n', '')
        stop = all_stops[i].split(' ')
        stops.append(stop)
    return stops

def findClosestHyperLoopStop(current_stop, p1, p2):
    distance_from_p1 = findDistanceBetweenPoints(current_stop, p1)
    distance_from_p2 = findDistanceBetweenPoints(current_stop, p2)
    if distance_from_p1 <= distance_from_p2:
        return p1
    else:
        return p2

def findStopFromList(stops, search):
    stop = []
    for i in range(len(stops)):
        if (stops[i][0] == search):
            stop = stops[i]
            stop.append(i)
            return stop

def findDistanceBetweenPoints(p1, p2):
    x1 = int(p1[1])
    y1 = int(p1[2])
    x2 = int(p2[1])
    y2 = int(p2[2])
    return math.sqrt( (x2 - x1)**2 + (y2 - y1)**2 )
       
def find_efficient_route(file_name):
    input = read_input_file(file_name)
    
    no_of_stops = int(input[0])
    no_of_journeys = int(input[no_of_stops + 1])

    stops = getStops(input, no_of_stops)

    hyperloop_locations = input[-1].replace('\n', '').split(' ')

    hyperloop_start = findStopFromList(stops, hyperloop_locations[0])
    hyperloop_stop = findStopFromList(stops, hyperloop_locations[1])
    count = 0
    for i in range(no_of_stops + 2, len(input)-1):
        journey_start = ''
        journey_stop = ''

        journey_start = input[i].split(' ')[0]
        journey_stop = input[i].split(' ')[1]

        current_time = int(input[i].split(' ')[2])

        journey_start_point = findStopFromList(stops, journey_start)
        journey_stop_point = findStopFromList(stops, journey_stop)

        closest_hyperloop_start = findClosestHyperLoopStop(journey_start_point, hyperloop_start, hyperloop_stop)
        closest_hyperloop_stop = findClosestHyperLoopStop(journey_stop_point, hyperloop_start, hyperloop_stop)

        driving_distance = findDistanceBetweenPoints(journey_start_point, closest_hyperloop_start)
        driviing_time = driving_distance / 15.0

        hyperloop_travel_distance = findDistanceBetweenPoints(closest_hyperloop_start, closest_hyperloop_stop)
        
        hyperloop_travel_time = hyperloop_travel_distance / 250 + 200

        walk_distance = findDistanceBetweenPoints(journey_stop_point, closest_hyperloop_stop)
        walk_time = walk_distance / 15.0

        total_journey_time = driviing_time + hyperloop_travel_time + walk_time
        if (current_time > total_journey_time):
            count = count + 1
    return count
