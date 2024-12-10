## GraphDB

1. Login to the your local GraphDB instance (running on Docker) via the following link:
http://localhost:7200/

![GraphDB First Screen](<Screenshots/GraphDB first screen.png>)

### After accessing the DB a repository should be created
1. Navigation side bar -> Setup -> Create new repository
![alt text](<Screenshots/Create repository.png>)
2. Select the opton of GraphDB repository (the first one from the left)
![alt text](<Screenshots/Create repository 2.png>)
3. Repository ID (any name you want)
![alt text](<Screenshots/Create repository 3.png>)
4. Leave all othe detailes as deafult and press Create button on the bottm right of the screen
![alt text](<Screenshots/Create repository 4.png>)
5. Now you will see the repository and will be able to select it on the combobox in the upper right corner of the screen
![alt text](<Screenshots/Create repository 5.png>)

### Importing files
1. Navigation side bar -> Import -> Upload RDF files
![alt text](<Screenshots/import file 1.png>)
2. Select TTL File (e.g. Datasets/TTLs/output.ttl)
Leave default values as is
![alt text](<Screenshots/import file 2.png>)
3. Press the Import button in the dialog and then you will see the data been loaded
![alt text](<Screenshots/import file 3.png>)


### Exploration 
1. Navigation side bar -> Explore -> Graph overview
2. the default graph (it's the same name every time and updated every time the imported file changes-no need for delete)
![alt text](<Screenshots/Exploration 1.png>)

3. Depending the situation you can go through subject-predicate-object-content to look data in detail
4. After selecting one you can see also the visualization through visual graph up right


### Sparql
1. First we have to turn on autocomplete
2. Navigation side bar -> Setup -> Autocomplete -> on
3. Navigation side bar -> Sparql to write queries-possibility of saving for future use


### Support
1. Through help you can find documentation and step-by-step tutorial
2. Except for that Interactive Guides have 2 examples that demonstrate all that mentioned step-by-step