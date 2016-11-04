var pg = require('pg');
var fs = require('fs');
const readline = require('readline');

var config = {
	user: 'postgres',
	password: 'abc12345',
	database: 'postgres',
	host: 'localhost',
	port: 5432
};

var client = new pg.Client(config);

client.on('drain', client.end.bind(client)); //disconnect client when all queries are finished
client.connect();
//Drop all tables.
function dropTables() {
	client.query('drop table c_driver_schedule; drop table c_driver_step; drop table c_driver_step_detail; drop table c_app_run_dependency; drop table c_driver_step_detail_h;', function(err, result) {
		// console.log('Result of dropTables(): ' + result);
	});
}

// Create all necessary tables.
function createTables() {
	// Table C_DRIVER_SCHEDULE.
	client.query("create table c_driver_schedule (" + 
		"audt_id decimal(18,0),"+
		"app_nme varchar(25),"+
		"run_nme varchar(25),"+
		"run_nbr integer,"+
		"re_run_nbr integer,"+
		"schdl_start_dtm timestamp,"+
		"stts_cd varchar(40),"+
		"vlutn_start_dtm timestamp,"+
		"vlutn_end_dtm timestamp,"+
		"run_start_dtm timestamp,"+
		"run_end_dtm timestamp,"+
		"crt_dtm timestamp,"+
		"lst_mdfd_dtm timestamp,"+
		"app_run_id decimal(18,0),"+
		"sla_date date,"+
		"sla_time timestamp);", function(erro, result) {
		// console.log(result);
	});
	// Table C_DRIVER_STEP.
	client.query('create table c_driver_step ('+
		'drvr_step_id decimal(18, 0),'+
		'app_nme varchar(25) not null,'+
		'run_nme varchar(25) not null,'+
		'grp_nbr smallint not null,'+
		'grp_nme varchar(80),'+
		'run_order_nbr smallint not null,'+
		'path_txt varchar(80) null,'+
		'cmd_txt varchar(80) null,'+
		'prmtr_txt varchar(256),'+
		'grp_cncrrncy_ind varchar(2),'+
		'step_cncrrncy_ind varchar(2),'+
		'notify_txt varchar(128),'+
		'step_typ_cd varchar(40),'+
		'step_nme varchar(128),'+
		'err_prcs_nbr smallint,'+
		'crt_dtm timestamp,'+
		'lst_mdfd_dtm timestamp,'+
		'app_run_id decimal(18,0),'+
		'actv_step_ind varchar(2));', function(err, result) {
		// console.log(result);
	});
	// Table C_DRIVER_STEP_DETAIL.
	client.query('create table c_driver_step_detail ('+
		'drvr_step_dtl_id decimal(18,0),'+
		'audt_id decimal(18,0),'+
		'drvr_step_id decimal(18,0),'+
		'app_name varchar(25) not null,'+
		'run_name varchar(25) not null,'+
		'grp_nbr smallint not null,'+
		'run_order_nbr smallint not null,'+
		'run_stts_cd varchar(2) null,'+
		'err_prcs_nbr smallint,'+
		'sess_start_dtm timestamp,'+
		'sess_end_dtm timestamp,'+
		'run_start_dtm timestamp,'+
		'run_end_dtm timestamp,'+
		'crt_dtm timestamp,'+
		'lst_mdfd_dtm timestamp);', function (err, result) {
		// console.log(result);
	});
	// Table C_APP_RUN_DEPENDENCY.
	client.query('create table c_app_run_dependency ('+
		'run_app_dpndnc_id decimal(18,0),'+
		'app_nme varchar(25) not null,'+
		'run_nme varchar(25) not null,'+
		'dependant_app_nme varchar(25) not null,'+
		'dependant_run_nme varchar(25) not null,'+
		'crt_dtm timestamp,'+
		'lst_mdfd_dtm timestamp,'+
		'app_run_id decimal(18,0),'+
		'dependant_app_run_id decimal(18,0));', function(err, result){
		// console.log(result);
	});
	// Table C_DRIVER_STEP_DETAIL_H.
	client.query('create table c_driver_step_detail_h ('+
		'drvr_step_dtl_id decimal(18,0),'+
		'audt_id decimal(18,0),'+
		'drvr_step_id decimal(18,0),'+
		'app_name varchar(25) not null,'+
		'run_name varchar(25) not null,'+
		'grp_nbr smallint not null,'+
		'run_order_nbr smallint not null,'+
		'run_stts_cd varchar(2) null,'+
		'err_prcs_nbr smallint,'+
		'sess_start_dtm timestamp,'+
		'sess_end_dtm timestamp,'+
		'run_start_dtm timestamp,'+
		'run_end_dtm timestamp,'+
		'crt_dtm timestamp,'+
		'lst_mdfd_dtm timestamp,'+
		'hist_dtm timestamp);', function(err, result) {
		// console.log(result);
	});
}
function test() {
	client.query("SELECT name FROM teamc", function(err, result) {
		console.log(result.rows);
	});
}

function loadTable(table_name) {
	// Open file to read.
	const rl = readline.createInterface({
		input: fs.createReadStream('./datalogfiles/' + table_name + '.txt')
		// input: fs.createReadStream('./datalogfiles/fake.txt')
	});
	var counter = 0;
	rl.on('line', (line) => {
				// process.exit(0);
				counter = counter + 1;
				if (counter === 1) return;
				// var f = formulateInsertMacro;
				var x = line;
				x = formulateInsertMacro(table_name, toArray(x));
				// console.log(`${x}`);
				client.query(x, function(err, result) {
					if(err) {
						console.log('erro bro: ' +err);
						console.log(x);
						process.exit(0);
					}
					else {
						console.log('result bro: ' + result);			
					}
				});

	});

	rl.on('close', () => {
		console.log('End reading lines from text file.');
		// Close database connection.
		// client.end();
		console.log('Program finished!');
	});

}

var dum = ['16600001	CLAIMS	CLM_CONV_SRC_STG	1000	M_CNG_VKX4490T_KEYS	1010		$UTILITY_SCRIPTS		udRunMacroNoP	pm_cng_psa_d m_cng_vkx4490t_keys	n	n	$APP_ERROR_MAIL_RECIP	UNIX	M_CNG_VKX4490T_KEYS	2	2010-05-27 14:33:11.110000	2013-06-12 12:24:06.940000	?	Y',
'21400010	CLAIMS	CLM_CONV_SRC_STG	1000	M_CNG_VKX3640T_KEYS	1020	$UTILITY_SCRIPTS	udRunMacroNoP	pm_cng_psa_d m_cng_vkx3640t_keys	n	n	$APP_ERROR_MAIL_RECIP	UNIX	M_CNG_VKX3640T_KEYS	2	2012-01-30 14:33:11.110000	2013-06-12 12:24:07.130000	?	Y',
'18700020	CLAIMS	CLM_ACS_EXTR	50	CLEANUP	10	$DRIVER_CONTROL	ud_driverCleanPrevRun		n	n	$APP_ERROR_MAIL_RECIP	UNIX	ud_driverCleanPrevRun	2	2010-06-10 07:00:00.000000	2010-06-10 07:00:00.000000	?	Y',
'18700021	CLAIMS	CLM_ACS_EXTR	100	DRIVER_ENTRIES_INSERT	10	$UTILITY_SCRIPTS	edwPMCMD	PM_CL_EXTR_COBRA wf_UPSERT_DRIVER_STEP_RUNNING 100 10	n	n	$APP_ERROR_MAIL_RECIP	UNIX	ud_driverCleanPrevRun	2	2010-06-10 07:00:00.000000	2010-06-10 07:00:00.000000	?	Y'];

dum.forEach((v) => {
	var splited_v = v.split('\t');
	if(splited_v.length !== 19) {
		splited_v = splited_v.filter((v) => {
			return v !=='';
		});
	}
	console.log(splited_v);
	console.log(splited_v.length);
})

function balance_parameters(line) {
	var splited_line = line.split('\t');
	if(splited_line.length !== 19) {
		splited_line = splited_line.filter((el) => {
			return el !=='';
		});
	}
	return splited_line;
}


function toArray(line) {
	var e_line = balance_parameters(line);
	for(var i = 0; i < e_line.length; i++) {
		if(e_line[i] === '?') {
			e_line[i] = 'DEFAULT';
		}
		else {
			e_line[i] = "'" + e_line[i] + "'";
			if(e_line[i] == '16600001') {
				process.exit(0);
			}
		}
	}
	return e_line;
}

function formulateInsertMacro(table, arr_values) {
	return ('insert into ' + table + ' values (' + arr_values + ');');
}

function loadTables() {
	// Fail because 'Driver Step ID' column is not in table schema from the log file.
	loadTable('c_driver_step');

	// Fail because of the header.
	// loadTable('c_driver_step_detail');

	// Fail because of the header.
	// loadTable('c_driver_step_detail_h');

	// Successful without the header.
	// loadTable('c_app_run_dependency');

	// Fail because the database schema lacks of the last two columns in the datalog.
	// loadTable('c_driver_schedule');
}

// Execute functions.
dropTables();
createTables();
loadTables();
// test();

