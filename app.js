const { json } = require('body-parser');
const { error, log } = require('console');
const { name } = require('ejs');
const fs = require('fs');
const mysql = require('mysql2');
const { type } = require('os');
const JSONStream = require('JSONStream');
const readline = require('readline');
const es = require('event-stream');



// FUNCTION FOR GENERATING FIELDS - Generates fields automatically by reading the objects key from the JSON file
//and pushes them while creating the table.

// function generateFieldDefinitions(obj, prefix = '') {
//     const cols = [];
//     for (const key in obj) {
//       if (typeof obj[key] === 'object') {
//         if(Array.isArray(obj[key])){
//             cols.push(`${prefix}${key} VARCHAR(255)`);
//         }else{
//             const nestedFields = generateFieldDefinitions(obj[key], `${prefix}${key}_`);
//             cols.push(...nestedFields);
//         }
//       } else {
//         cols.push(`${prefix}${key} VARCHAR(255)`);
//       }
//     }
//     return cols;
//   }

// const cols = generateFieldDefinitions(jsonData[0]);
// const createTableQuery = `CREATE TABLE IF NOT EXISTS ${table_name}(   // CREATING TABLE
//     ${cols.join(',\n')}
// )`;




// CODE FOR ALTERING TABLE AND ADDING FIELDS - can be used to insert fields in bulk at once into a table

// fields=[
    
//     'roll',
//     'FatherName',
//     'FatherOccu',
//     'FatherPh',
//     'FatherEmail',
//     'FatherPin',
//     'FatherLoc',
//     'FatherSt1',
//     'FatherSt2',
//     'FatherCity',
//     'FatherDistrict',
//     'FatherState', 
//     'FatherCountry',
//     'MotherName', 
//     'MotherOccu',
//     'MotherPh',
//     'MotherEmail',
//     'MotherPin',
//     'MotherLoc',
//     'MotherSt1',
//     'MotherSt2',
//     'MotherCity',
//     'MotherDistrict',
//     'MotherState',
//     'MotherCountry',
//     'GuardianName',
//     'GuradianOccu',
//     'GuardianEmail',
//     'GuardianAdd'
            
//     ]

// for(field of fields){
//     con.query(`alter table familydata add column ${field} varchar(255)`, function(err, res){
//         if(err) console.log(err);
//         else console.log("fields inserted ", res);
//     })
// }


// STUDENT DATABASE

//Creating a connection with database "students" using createConnection
var studentCon = mysql.createConnection({
    host: "localhost",
    port: "3306",
    database: "students",
    user: "root",
});

studentCon.connect(function(err){
    if(err) console.log(err);                               //Logs error if any encounters any error
    else console.log("Connected to MySQL successfully!");   //Logs the message of connection successfully made
});


const studentPath = './Student.json';                       //Path of the Student Info JSON file
const stream = fs.createReadStream(studentPath, { encoding: 'utf8' });   //Creating a read stream using fs module
const studentJsonStream = JSONStream.parse('*');    
stream.pipe(studentJsonStream);                             //Piping/Streaming the data

studentJsonStream.on('data', (data) => {                    //Initializing the Stream
    //Tapping into the data by creating path with the objects required of the JSON data file to be inserted into the table  
    const {
        _id, 
        _rev, 
        roll, 
        name, 
        email
    } = data;
    const {
        major: Major, 
        minor: Minor, 
        minorTwo: minTwo, 
        statusDate: statDate, 
        addDegree: addDeg, 
        graduationDate: gradDate, 
        addDegreeDate: addDegDate, 
        degree: deg, 
        degreeDate: degDate, 
        admissionDate: admDate, 
        status: Stat, 
        prog: Prog} = data.acadIISER;
    const {
        gender: Gender,
        dob: Dob,
        religion: Religion,
        aadhaarNo: AadharNo,
        phone: Phone,
        altPhone: AltPhone,
        altEmail: altEmail,
        placeOfBirth: PlaceOfBirth,
        bloodGroup: BloodGroup,
        identificationMark: IdentificationMark
    } = data.personal
    const{
        main: Main,
        admCat: AdmCat,
        phyDisabled: phyDisabled,
        jkMigrant: JkMigrant,
        ecoBackward: EcoBackward,
        exServiceman: ExServiceman
    } = data.category
    const{
        name: EditByName,
        role: EditByRole,
        route: EditRoute,
        time: EditTime,
        browser: EditBrowser,
        os: EditOS,
        ip: EditIp
    } = data.editBy
    //Running the insert query operation to insert personal data of students into the student table
    studentCon.query(`INSERT into personaldata values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
        [
            _id,
            _rev,
            roll,
            name, 
            email,
            Major,
            Minor,
            minTwo,
            statDate,
            addDeg,
            gradDate,
            addDegDate,
            deg,
            degDate,
            admDate,
            Stat,
            Prog,
            Gender,
            Dob,
            Religion,
            AadharNo,
            Phone,
            AltPhone,
            altEmail,
            PlaceOfBirth,
            BloodGroup,
            IdentificationMark,
            Main,
            AdmCat,
            phyDisabled,
            JkMigrant,
            EcoBackward,
            ExServiceman,
            EditByName,
            EditByRole,
            EditRoute,
            EditTime,
            EditBrowser,
            EditOS,
            EditIp
        ], 
        function(err, res){
        if(err) console.log("Error occurred while inserting data of students",err);           //Logs error if the process encounters any error
        else console.log("student data inserted successfully", res);     //Logs message if the data is inserted successfully 
    })
})

//Starting read stream for inserting father, mother and guradian data into the family table
studentJsonStream.on('data', (fam) =>{
//Tapping into the data by creating path of the objects required from the JSON data file to be inserted into the family table  
    const {
        roll
    } = fam;
    const{
        name: FatherName,
        occupation: FatherOccu,
        phone: FatherPh,
        email: FatherEmail,
    } = fam.father;
    const{pincode: FatherPin,
        locality: FatherLoc,
        street1: FatherSt1,
        street2: FatherSt2,
        city: FatherCity,
        district: FatherDistrict,
        state: FatherState,
        country: FatherCountry} = fam.father.address;
    const {
        name: MotherName,
        occupation: MotherOccu,
        phone: MotherPh,
        email: MotherEmail,
        } = fam.mother;

    const {pincode: MotherPin,
        locality: MotherLoc,
        street1: MotherSt1,
        street2: MotherSt2,
        city: MotherCity,
        district: MotherDistrict,
        state: MotherState,
        country: MotherCountry} = fam.mother.address;

    const{
        name: GuardianName,
        occupation: GuradianOccu,
        phone: GuardianPh,
        email: GuardianEmail,
    } = fam.guardian;

    const {address: GuardianAdd} = fam.guardian
//Query to insert data of family members into the family table
    studentCon.query(`insert into familydata values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    [
        0,
        roll,
        FatherName,
        FatherOccu,
        FatherPh,
        FatherEmail,
        FatherPin,
        FatherLoc,
        FatherSt1,
        FatherSt2,
        FatherCity,
        FatherDistrict,
        FatherState, 
        FatherCountry,
        MotherName, 
        MotherOccu,
        MotherPh,
        MotherEmail,
        MotherPin,
        MotherLoc,
        MotherSt1,
        MotherSt2,
        MotherCity,
        MotherDistrict,
        MotherState,
        MotherCountry,
        GuardianName,
        GuradianOccu,
        GuardianPh,
        GuardianEmail,
        JSON.stringify(GuardianAdd)
    ], function(err,res){
        if(err) console.log("Error occurred while inserting family data",err);                           //Logs error if the code encounters any error
        else console.log("Family Data inserted", res);      //Logs message of data inserted successfully
    });

});

//Starting read stream to read bank details from the JSON data
studentJsonStream.on('data', (Bank) => {
    const {
        ifsc: Ifsc,
        bank: Bank,
        account: Account,
        branch: Branch
    } = Bank.bank;
    const {roll} = Bank;
    //Query to insert bank details into the bank table
    studentCon.query(`INSERT ignore into bank values (?.?,?,?,?,?)`, 
    [
        0,
        roll,
        Ifsc,
        Bank,
        Account,
        Branch
    ], function(err, res){
        if(err) console.log("Error occurred while inserting bank details",err);     //Logs error message if encounters any error 
        else console.log("Bank Data inserted", res);                                //Logs message of data inserted successfully
    });
});



// Creating stream for inserting report data
studentJsonStream.on('data', (data) => {
    const{
        type: Type,
        sem: Sem,
        annotation: Annotation,
        file: File,
        show: Show
    } = data.reports
    const{roll} = data;
    for(const report of reports){
        for(const oneRepo of report){
            //Query for inserting report data into reports table
            studentCon.query(`insert ignore into reports value (?,?,?,?,?,?,?)`,
            [
                0,
                roll,
                Type,
                Sem,
                Annotation,
                File,
                Show
            ], function(err, res){
                if(err) console.log("Error ecountered while inserting report ",err);                       //Logs error message if encounters any error 
                else console.log("report data inserted", res);  //Logs message if data inserted successfully
            });
        }
    }
});


//  USER DATABASE 

// Creating connection with User database
var userCon = mysql.createConnection({
    host: "localhost",
    port: "3306",
    database: "user",
    user: "root",
});

//Connecting with user database
userCon.connect(function(err){
    if(err) console.log("Could not create connection with user databae",err);   //Logs error if connection fails
    else console.log("Connected to MySQL User Database successfully!");         //Logs message if connection is established successfully 
});

const userPath = './user.json';         //Path of JSON file containing user info
const userStream = fs.createReadStream(userPath, { encoding: 'utf8' });
const userJsonStream = JSONStream.parse('*');
stream.pipe(userJsonStream);

//Stream for inserting userid data
userJsonStream.on('data', (data) => {
    const {
        _id,
        _rev,
        roll,
        name,
        email,
        idType,
        role,
        db,
        secret,
        token,
        forgotToken,
        disable
    } = data;

    //Query for inserting user data into userid table
    userCon.query(`insert ignore into userid values ?`,
     [
        _id,
        _rev,
        roll,
        name,
        email,
        idType,
        role,
        db,
        secret,
        token,
        forgotToken,
        disable
     ],
      function(err, res){
        if(err) console.log("Error while inserting user data",err);     //Logs error if error occurs                   
        else console.log("user data inserted successfully", res);       //Logs message when data inserted successfully
    })
})


// REGISTRATION DATABASE

//Creating connection for registration database
var registerCon = mysql.createConnection({
    host: "localhost",
    port: "3306",
    database: "registration",
    user: "root",
});

//Connecting to registration database
registerCon.connect(function(err){
    if(err) console.log("Error occurred whlie creating registration database",err);  //Logs error if error encounters
    else console.log("Connected to MySQL successfully!");                            //Logs success message                        
});


const registerPath = './registration.json';     // Path of JSON file containing registration info
const registerStream = fs.createReadStream(registerPath, { encoding: 'utf8' });
const registerJsonStream = JSONStream.parse('*');
stream.pipe(registerJsonStream);

//Starting stream for reading registration data
registerJsonStream.on('data', (data) => {
    const {
        _id,
        _rev,
        roll,
        name,
        department,
        program,
        sem,    
        acadYear,
        currentYear,
        receivedByAA,
        regDate
    } = data;
    const{
        date: pay_date,
        bankRef: pay_ref,
        amount: pay_amount
    } = data.payment;
    const {
        name: Editby_name,
        role: Editby_role,
        route: Editby_route,
        time: Editby_time,
        browser: Editby_browser,
        os: Editby_os,
        ip: Editby_ip
    } = data.editBy;
    //Query for inserting registration info into the registration table
    registerCon.query(`insert into registrationinfo values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    [
        0,
        _id,
        _rev,
        roll,
        name,
        department,
        program,
        sem,
        acadYear,
        currentYear,
        pay_date,
        pay_ref,
        pay_amount,
        receivedByAA,
        Editby_name,
        Editby_role,
        Editby_route,
        Editby_time,
        Editby_browser,
        Editby_os,
        Editby_ip,
        regDate
    ], 
    function(err,res){
        if(err) console.log("Error encountered while inserting registration info",err);   //Logs error if error occurs
        else{
            console.log("registrationinfo inserted successfully", res);  //logs success message
            tableId = res.insertId;         // Taking regpk(0) inside a variable to push into coursereg table
            for(const course of data.registration){
                const{
                    cnum: cNum,
                    cname: cName,
                    credits: Credits,
                    grade: Grade,
                    type: Type,
                    approve: Approve
                } = course;
                const{roll} = data;
                //Query for inserting registered course data into coursereg table
                registerCon.query(`insert into coursereg values (?,?,?,?,?,?,?,?,?)`,
                [
                    0,
                    tableId,
                    roll,
                    cNum,
                    cName,
                    Credits,
                    Grade,
                    Type,
                    Approve
                ], function(err,res){
                    if(err) console.log("Error occured while inserting coursereg data",err);    //Logs error if encounters an error
                    else console.log("course registered data inserted successfully", res);      //Logs success message
                })
            }
        } 
    });
});

// PERFORMANCE TABLE DATA


const performancePath = './student.json';   //Path of performance data  
const performanceStream = fs.createReadStream(performancePath, { encoding: 'utf8' });
const performJsonStream = JSONStream.parse('*');
stream.pipe(performJsonStream);


let regpk = [];

//Starting read stream for 
jsonStream.on('data', (data) => {
    const {
        acadYear: AcadYear,
        attemptedCredits: AttemptedCredits,
        obtainedCredits: ObtainedCredits,
        cpi: Cpi,
        improved: Improved,
        currentYear: CurrentYear,
        failedCourses: FailedCourses,
        passedCourses: PassedCourses 
    } = data.current;
    const {roll} = data;
    //Query for importing regpk from registrationinfo table 
    registerCon.query(`select regpk from registrationinfo where roll=?`, roll, function(err, res){
        if(err) console.log(err);
        else{
            const row = res[0];
            const reg = row.regpk;
            //Query for inserting performance data into performance table
            registerCon.query(`insert into performance values (?,?,?,?,?,?,?,?,?,?,?)`,
        [
            0,
            reg,
            roll,
            AcadYear,
            AttemptedCredits,
            ObtainedCredits,
            Cpi,    
            Improved,
            CurrentYear,
            JSON.stringify(FailedCourses).toString(),
            JSON.stringify(PassedCourses).toString()
        ], function(err, res){
            if(err) console.log("Error encountered while inserting performance data",err);      //Logs error 
            else console.log("studentperformance data  inserted",res);      //logs success message
        })
        }
    })
    
});