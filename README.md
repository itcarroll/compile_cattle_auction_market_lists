compile_cattle_auction_market_lists
===================================

Project summary on the Harvard Dataverse Network: [10.7910/DVN/28209](http://dx.doi.org/10.7910/DVN/28209)

The module `compile_market.py` contains the two functions used to (1) assign markets to a common premises id, and (2) locate the county containing each premises.
The purpose of this repository is to disclose the methods used, rather than create a stand-alone application.
First of all, the user must supply API keys for geonames.org and developer.mapquest.com to use the second function.
Second, and more importantly, the function takes the argument `session`, which must be an instance of sqlalchemy.orm.session.Session() connected to a MySQL database containing the un-compiled data from the original sources.
