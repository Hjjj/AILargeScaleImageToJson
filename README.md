This App calls Azure Ai Services to convert AHA image cards to JSON data. 
The service starts up and scans a directory of jpeg images and puts a list of all those files into a work queue. 
The service loops through the work queue and sends the images to Azure for conversion. 
Utilizes logging and a stateful work queue
