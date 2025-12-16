# The following code will only execute
# successfully when compression is complete

import kagglehub

# Download latest version
path = kagglehub.dataset_download("ngocbaotrinhtuan/movies-len-32m")

print("Path to dataset files:", path)