
INSERT INTO wh_db_stage.ProspectIncremental (
    agencyid, lastname, firstname, middleinitial, gender, addressline1, 
    addressline2, postalcode, city, state, country, phone, income, 
    numbercars, numberchildren, maritalstatus, age, creditrating, 
    ownorrentflag, employer, numbercreditcards, networth, 
    marketingnameplate, recordbatchid, batchid
)
SELECT
    tp.agencyid, tp.lastname, tp.firstname, tp.middleinitial, tp.gender, tp.addressline1, 
    tp.addressline2, tp.postalcode, tp.city, tp.state, tp.country, tp.phone, tp.income, 
    tp.numbercars, tp.numberchildren, tp.maritalstatus, tp.age, tp.creditrating, 
    tp.ownorrentflag, tp.employer, tp.numbercreditcards, tp.networth, 
    tp.marketingnameplate, tp.batchid, tp.batchid
FROM temp_propect_marketingnameplate AS tp
ON CONFLICT (agencyid, lastname, firstname) DO UPDATE SET
    middleinitial = excluded.middleinitial,
    gender = excluded.gender,
    addressline1 = excluded.addressline1,
    addressline2 = excluded.addressline2,
    postalcode = excluded.postalcode,
    city = excluded.city,
    state = excluded.state,
    country = excluded.country,
    phone = excluded.phone,
    income = excluded.income,
    numbercars = excluded.numbercars,
    numberchildren = excluded.numberchildren,
    maritalstatus = excluded.maritalstatus,
    age = excluded.age,
    creditrating = excluded.creditrating,
    ownorrentflag = excluded.ownorrentflag,
    employer = excluded.employer,
    numbercreditcards = excluded.numbercreditcards,
    networth = excluded.networth,
    marketingnameplate = excluded.marketingnameplate,
    recordbatchid = excluded.batchid;
