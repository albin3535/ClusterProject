import csv
import math
from datetime import datetime
from time import time

def calculate_distance(x1, y1, x2, y2):
    return math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2)


def cluster_data(input_file, output_file):
    clusters = []

    with open(input_file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            timestamp = datetime.strptime(row['timestamp_id'], '%Y-%m-%dT%H:%M:%S.%fZ')
            sensor_id = row['sensor_id']
            unique_id = int(row['unique_id'])
            x_position = float(row['x_position'])
            y_position = float(row['y_position'])
            entry = {
                'timestamp': timestamp,
                'sensor_id': sensor_id,
                'unique_id': unique_id,
                'x_position': x_position,
                'y_position': y_position
            }

            matched_clusters = []
            for cluster in clusters:
                for obj in cluster:
                    if obj['unique_id'] == unique_id:
                        matched_clusters.append(cluster)
                        break

            if not matched_clusters:
                clusters.append([entry])
            else:
                merged_cluster = [entry]
                for cluster in matched_clusters:
                    merged_cluster.extend(cluster)
                    clusters.remove(cluster)
                clusters.append(merged_cluster)

    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = ['f_timestamp', 'f_id', 'cluster_data', 'f_u_id']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for cluster in clusters:

            f_timestamp = sum(obj['timestamp'].timestamp() for obj in cluster) / len(cluster)
            f_id = hash(tuple(obj['sensor_id'] for obj in cluster))
            cluster_data = [[obj['x_position'], obj['y_position'], obj['sensor_id']] for obj in cluster]
            f_u_id = next((obj['unique_id'] for obj in cluster if obj['unique_id'] != 0), None)

            writer.writerow({
                'f_timestamp': f_timestamp,
                'f_id': f_id,
                'cluster_data': cluster_data,
                'f_u_id': f_u_id
            })
def main():
    input_file = 'C:\\cluster\\test_Data.csv'
    output_file = 'output_data.csv'
    cluster_data(input_file, output_file)
if __name__ == "__main__":

    main()