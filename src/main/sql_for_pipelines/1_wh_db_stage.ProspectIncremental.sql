
CREATE OR REPLACE TEMP TABLE temp_propect AS 
SELECT     *, 
    1 AS batchid 
    FROM read_csv_auto('src/data/Batch1\Prospect.csv', columns={
    "agencyid": "STRING",
    "lastname": "STRING",
    "firstname": "STRING",
    "middleinitial": "STRING",
    "gender": "STRING",
    "addressline1": "STRING",
    "addressline2": "STRING",
    "postalcode": "STRING",
    "city": "STRING",
    "state": "STRING",
    "country": "STRING",
    "phone": "STRING",
    "income": "INT",
    "numbercars": "INT",
    "numberchildren": "INT",
    "maritalstatus": "STRING",
    "age": "INT",
    "creditrating": "INT",
    "ownorrentflag": "STRING",
    "employer": "STRING",
    "numbercreditcards": "INT",
    "networth": "INT",
}, header=False);
