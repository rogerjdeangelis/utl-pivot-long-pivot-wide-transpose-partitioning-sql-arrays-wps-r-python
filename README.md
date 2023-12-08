# utl-pivot-long-pivot-wide-transpose-partitioning-sql-arrays-wps-r-python
Pivoting long to wide and wide to long in sql wps r and python
    %let pgm=utl-pivot-long-pivot-wide-transpose-partitioning-sql-arrays-wps-r-python;

    Pivoting long to wide and wide to long in sql wps r and python

    Depending on the compiler these solutions may not be slow.

         PIVOT LONG

              1. wps sql hardcoded
              2. wps sql dynamic
              3. wps r sql dynamic
              4. wps python dynamic

         PIVOT WIDE

              1. wps sql dynamic
              2. wps r sql dynamic
              3. wps python dynamic

              github                                                                                                      
              https://tinyurl.com/229d8w66                                                                                
              https://github.com/rogerjdeangelis/utl-pivot-long-pivot-wide-transpose-partitioning-sql-arrays-wps-r-python 


    /*___ _____     _____ _____   _     ___  _   _  ____
    |  _ \_ _\ \   / / _ \_   _| | |   / _ \| \ | |/ ___|
    | |_) | | \ \ / / | | || |   | |  | | | |  \| | |  _
    |  __/| |  \ V /| |_| || |   | |__| |_| | |\  | |_| |
    |_|  |___|  \_/  \___/ |_|   |_____\___/|_| \_|\____|


    PROLEM

    /*************************************************************************************************************************/
    /*                                |                                                                 |                    */
    /*INPUT                           | PROCESS CREATE SQL MACRO CODE ARRAY                             |       OUTPUT       */
    /*                                |                                                                 |                    */
    /*                                |                                                                 |                    */
    /* TP  A1 A2 A3 B1 B2 B3 C1 C2 C3 | %array(_vr,values=%varlist(data=sd1.have,drop=TIME))            | TP PATIENT DOSEAGE */
    /*                                |                                                                 |                    */
    /*  1  40 40 20 20 10 14 25 24 28 |                                                                 |  1   A1       40   */
    /*  2  50 50 40 40 20 22 30 30 33 | proc sql;                                                       |  1   A2       40   */
    /*  3  60 60 50 50 30 37 41 41 47 |   %do_over(_vr,phrase=%str(put "selest TIMEPOINT`?` as PATIENT  |  1   A3       20   */
    /*  4  70 70 60 60 40 45 49 50 51 |    , ? as DOSEAGE from sd1.havFat union corr";));               |  1   B1       20   */
    /*  5  80 80 70 70 50 52 64 63 67 | quit;                                                           |  1   B2       10   */
    /*  6  90 90 80 80 60 65 71 72 72 |                                                                 |  1   B3       14   */
    /*  7  70 70 60 60 40 45 49 50 51 | This is what the code above create                              |  1   C1       25   */
    /*                                |                                                                 |  1   C2       24   */
    /*                                | select TP, 'A1' as PATIENT, A1 as DOSEAGE from havFat union corr|  1   C3       28   */
    /*                                | select TP, 'A2' as PATIENT, A2 as DOSEAGE from havFat union corr|  2   A1       50   */
    /*                                | select TP, 'A3' as PATIENT, A3 as DOSEAGE from havFat union corr|  2   A2       50   */
    /*                                | select TP, 'B1' as PATIENT, B1 as DOSEAGE from havFat union corr|  2   A3       40   */
    /*                                | select TP, 'B2' as PATIENT, B2 as DOSEAGE from havFat union corr|  2   B1       40   */
    /*                                | select TP, 'B3' as PATIENT, B3 as DOSEAGE from havFat union corr|  2   B2       20   */
    /*                                | select TP, 'C1' as PATIENT, C1 as DOSEAGE from havFat union corr|  2   B3       22   */
    /*                                | select TP, 'C2' as PATIENT, C2 as DOSEAGE from havFat union corr|  2   C1       30   */
    /*                                | select TP, 'C3' as PATIENT, C3 as DOSEAGE from havFat           |  2   C2       30   */
    /*                                |                                                                 |  2   C3       33   */
    /*                                |                                                                 |                    */
    /*                                |                                                                 |                    */
    */ /**********************************************************************************************************************/

    /*
     _                   _              _     _
    (_)_ __  _ __  _   _| |_  __      _(_) __| | ___
    | | `_ \| `_ \| | | | __| \ \ /\ / / |/ _` |/ _ \
    | | | | | |_) | |_| | |_   \ V  V /| | (_| |  __/
    |_|_| |_| .__/ \__,_|\__|   \_/\_/ |_|\__,_|\___|
            |_|
    */

    data sd1.havfat;
     input TIME A1 A2 A3 B1 B2 B3 C1 C2 C3;
    cards4;
    1 40 40 20 20 10 14 25 24 28
    2 50 50 40 40 20 22 30 30 33
    3 60 60 50 50 30 37 41 41 47
    4 70 70 60 60 40 45 49 50 51
    5 80 80 70 70 50 52 64 63 67
    6 90 90 80 80 60 65 71 72 72
    7 100 100 90 90 70 73 83 80 88
    ;;;;
    run;quit;
     _                                  _   _                   _               _          _
    / | __      ___ __  ___   ___  __ _| | | |__   __ _ _ __ __| | ___ ___   __| | ___  __| |
    | | \ \ /\ / / `_ \/ __| / __|/ _` | | | `_ \ / _` | `__/ _` |/ __/ _ \ / _` |/ _ \/ _` |
    | |  \ V  V /| |_) \__ \ \__ \ (_| | | | | | | (_| | | | (_| | (_| (_) | (_| |  __/ (_| |
    |_|   \_/\_/ | .__/|___/ |___/\__, |_| |_| |_|\__,_|_|  \__,_|\___\___/ \__,_|\___|\__,_|
                 |_|                 |_|
    */

    /*----                                                                   ----*/
    /*----  generate select union code                                       ----*/
    /*----                                                                   ----*/

    %utl_submit_wps64x('

    libname sd1 "d:/sd1";
    options validvarname=any sasautos=("c:/otowps");

    %array(_vr,values=%varlist(data=sd1.have,drop=TIME));

    data _null_;
      %do_over(_vr,phrase=%str(put "select timepoint, `?` as patient, ? as DOSEAGE from sd1.havFat union corr";));
    run;quit;
    /*----                                                                   ----*/
    /*---- clead delete macro array                                          ----*/
    /*----                                                                   ----*/
    %arraydelete(_vr);
    ');

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* select timepoint, 'A1' as patient, A1 as DOSEAGE from sd1.havFat union corr                                              */
    /* select timepoint, 'A2' as patient, A2 as DOSEAGE from sd1.havFat union corr                                              */
    /* select timepoint, 'A3' as patient, A3 as DOSEAGE from sd1.havFat union corr                                              */
    /* select timepoint, 'B1' as patient, B1 as DOSEAGE from sd1.havFat union corr                                              */
    /* select timepoint, 'B2' as patient, B2 as DOSEAGE from sd1.havFat union corr                                              */
    /* select timepoint, 'B3' as patient, B3 as DOSEAGE from sd1.havFat union corr                                              */
    /* select timepoint, 'C1' as patient, C1 as DOSEAGE from sd1.havFat union corr                                              */
    /* select timepoint, 'C2' as patient, C2 as DOSEAGE from sd1.havFat union corr                                              */
    /* select timepoint, 'C3' as patient, C3 as DOSEAGE from sd1.havFat union corr                                              */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*----                                                                   ----*/
    /*----  cut and past the code from log                                   ----*/
    /*----                                                                   ----*/

    proc datasets lib=sd1 nolist nodetails;delete pivot_long; run;quit;

    %utl_submit_wps64x("
    libname sd1 'd:/sd1';
    options validvarname=any sasautos=('c:/otowps');
    proc sql;
       create
         table sd1.pivot_long as
       select timepoint, 'A1' as patient, A1 as DOSEAGE from sd1.havFat union corr
       select timepoint, 'A2' as patient, A2 as DOSEAGE from sd1.havFat union corr
       select timepoint, 'A3' as patient, A3 as DOSEAGE from sd1.havFat union corr
       select timepoint, 'B1' as patient, B1 as DOSEAGE from sd1.havFat union corr
       select timepoint, 'B2' as patient, B2 as DOSEAGE from sd1.havFat union corr
       select timepoint, 'B3' as patient, B3 as DOSEAGE from sd1.havFat union corr
       select timepoint, 'C1' as patient, C1 as DOSEAGE from sd1.havFat union corr
       select timepoint, 'C2' as patient, C2 as DOSEAGE from sd1.havFat union corr
       select timepoint, 'C3' as patient, C3 as DOSEAGE from sd1.havFat
    ;quit;
    proc print;
    run;quit;
    ");

    proc print data=sd1.pivot_long ;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  The WPS System                                                                                                        */
    /*                                                                                                                        */
    /*  Obs    TIMEPOINT    patient    DOSEAGE                                                                                */
    /*                                                                                                                        */
    /*    1        1          A1          40                                                                                  */
    /*    2        1          A2          40                                                                                  */
    /*    3        1          A3          20                                                                                  */
    /*    4        1          B1          20                                                                                  */
    /*    5        1          B2          10                                                                                  */
    /*    6        1          B3          14                                                                                  */
    /*    7        1          C1          25                                                                                  */
    /*    8        1          C2          24                                                                                  */
    /*    9        1          C3          28                                                                                  */
    /*   10        2          A1          50                                                                                  */
    /*   11        2          A2          50                                                                                  */
    /*   12        2          A3          40                                                                                  */
    /*   13        2          B1          40                                                                                  */
    /*   14        2          B2          20                                                                                  */
    /*   15        2          B3          22                                                                                  */
    /*   16        2          C1          30                                                                                  */
    /*   17        2          C2          30                                                                                  */
    /*   18        2          C3          33                                                                                  */
    /*   ....                                                                                                                 */
    /*                                                                                                                        */
    /**************************************************************************************************************************/


    /*___                                   _       _                             _
    |___ \  __      ___ __  ___   ___  __ _| |   __| |_   _ _ __   __ _ _ __ ___ (_) ___
      __) | \ \ /\ / / `_ \/ __| / __|/ _` | |  / _` | | | | `_ \ / _` | `_ ` _ \| |/ __|
     / __/   \ V  V /| |_) \__ \ \__ \ (_| | | | (_| | |_| | | | | (_| | | | | | | | (__
    |_____|   \_/\_/ | .__/|___/ |___/\__, |_|  \__,_|\__, |_| |_|\__,_|_| |_| |_|_|\___|
                     |_|                 |_|          |___/
    */


    proc datasets lib=sd1 nolist nodetails;delete pivot_long; run;quit;

    %utl_submit_wps64x('

    libname sd1 "d:/sd1";
    options validvarname=any sasautos=("c:/otowps");

    %array(_vr,values=%varlist(data=sd1.have,drop=TIME));

    proc sql;
       create
         table sd1.pivot_long as
      %do_over(_vr,phrase=%str(select timepoint, "?" as patient, ? as DOSEAGE from sd1.havFat), between=union corr)
    ;quit;

    proc print;
    run;quit;
    %arraydelete(_vr);
    ');

    proc print data=sd1.pivot_long;
    run;quit;


    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  The WPS System                                                                                                        */
    /*                                                                                                                        */
    /*  Obs    TIMEPOINT    patient    DOSEAGE                                                                                */
    /*                                                                                                                        */
    /*    1        1          A1          40                                                                                  */
    /*    2        1          A2          40                                                                                  */
    /*    3        1          A3          20                                                                                  */
    /*    4        1          B1          20                                                                                  */
    /*    5        1          B2          10                                                                                  */
    /*    6        1          B3          14                                                                                  */
    /*    7        1          C1          25                                                                                  */
    /*    8        1          C2          24                                                                                  */
    /*    9        1          C3          28                                                                                  */
    /*   10        2          A1          50                                                                                  */
    /*   11        2          A2          50                                                                                  */
    /*   12        2          A3          40                                                                                  */
    /*   13        2          B1          40                                                                                  */
    /*   14        2          B2          20                                                                                  */
    /*   15        2          B3          22                                                                                  */
    /*   16        2          C1          30                                                                                  */
    /*   17        2          C2          30                                                                                  */
    /*   18        2          C3          33                                                                                  */
    /*   ....                                                                                                                 */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*____                                   _                             _
    |___ /  __      ___ __  ___   _ __    __| |_   _ _ __   __ _ _ __ ___ (_) ___
      |_ \  \ \ /\ / / `_ \/ __| | `__|  / _` | | | | `_ \ / _` | `_ ` _ \| |/ __|
     ___) |  \ V  V /| |_) \__ \ | |    | (_| | |_| | | | | (_| | | | | | | | (__
    |____/    \_/\_/ | .__/|___/ |_|     \__,_|\__, |_| |_|\__,_|_| |_| |_|_|\___|
                     |_|                       |___/
    */

    proc datasets lib=sd1 nolist nodetails;delete pivot_long; run;quit;

    %utl_submit_wps64x("
    libname sd1 'd:/sd1';

    options validvarname=any;

    %array(_vr,values=%varlist(data=sd1.have,drop=TIME));

    proc r;
    export data=sd1.havfat r=havfat;
    submit;
    library(sqldf);

    pivot_long <- sqldf('
      %do_over(_vr,phrase=%str(select timepoint, \`?\` as patient, ? as DOSEAGE from havfat), between=union)
    ');

     pivot_long;
    endsubmit;
    import data=sd1.pivot_long r=pivot_long;
    run;quit;
    proc print data=sd1.pivot_long;
    run;quit;
    ");

    proc print data=sd1.pivot_long;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  The WPS R System                                                                                                      */
    /*                                                                                                                        */
    /*     TIMEPOINT patient DOSEAGE                                                                                          */
    /*  1          1      A1      40                                                                                          */
    /*  2          1      A2      40                                                                                          */
    /*  3          1      A3      20                                                                                          */
    /*  4          1      B1      20                                                                                          */
    /*  5          1      B2      10                                                                                          */
    /*  6          1      B3      14                                                                                          */
    /*  7          1      C1      25                                                                                          */
    /*  8          1      C2      24                                                                                          */
    /*  9          1      C3      28                                                                                          */
    /*  10         2      A1      50                                                                                          */
    /*  11         2      A2      50                                                                                          */
    /*  12         2      A3      40                                                                                          */
    /*  13         2      B1      40                                                                                          */
    /*  14         2      B2      20                                                                                          */
    /*  15         2      B3      22                                                                                          */
    /*  16         2      C1      30                                                                                          */
    /*  17         2      C2      30                                                                                          */
    /*  18         2      C3      33                                                                                          */
    /*                                                                                                                        */
    /**************************************************************************************************************************/
    /*  _                                      _   _                             _       _                             _
    | || |   __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |   __| |_   _ _ __   __ _ _ __ ___ (_) ___
    | || |_  \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |  / _` | | | | `_ \ / _` | `_ ` _ \| |/ __|
    |__   _|  \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | | | (_| | |_| | | | | (_| | | | | | | | (__
       |_|     \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|  \__,_|\__, |_| |_|\__,_|_| |_| |_|_|\___|
                      |_|         |_|    |___/                                |_|          |___/
    */
    proc datasets lib=sd1 nolist mt=data mt=view nodetails;delete pivot_long; run;quit;

    %utlfkil(d:/xpt/want.xpt);

    %utl_submit_wps64x("
    options validvarname=any lrecl=32756;
    libname sd1 'd:/sd1';
    %array(_vr,values=%varlist(data=sd1.have,drop=TIME));
    proc python;
    export data=sd1.havfat python=havfat;
    submit;
    import pyreadstat;
    from os import path;
    import pandas as pd;
    import numpy as np;
    import pandas as pd;
    from pandasql import sqldf;
    mysql = lambda q: sqldf(q, globals());
    from pandasql import PandaSQL;
    pdsql = PandaSQL(persist=True);
    sqlite3conn = next(pdsql.conn.gen).connection.connection;
    sqlite3conn.enable_load_extension(True);
    sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
    mysql = lambda q: sqldf(q, globals());
    want = pdsql('''
      %do_over(_vr,phrase=%str(select timepoint as TP, `?` as PATIENT, ? as DOSEAGE from havfat), between=union)
    ''');
    print(want);
    pyreadstat.write_xport(want, 'd:/xpt/want.xpt',table_name='want',file_format_version=5
    ,column_labels=['TIMEPOINT', 'PATIENT', 'DOSEAGE' ]);
    endsubmit;
    run;quit;
    ");

    /*--- handles long variable names by using the label to rename the variables  ----*/

    libname xpt xport "d:/xpt/want.xpt";
    proc contents data=xpt._all_;

    data want_py_long_names;
      %utl_rens(xpt.want) ;
      set want;
    run;quit;
    libname xpt clear;

    proc print data=want_py_long_names;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  The PYTHON Procedure                                                                                                  */
    /*                                                                                                                        */
    /*       TP PATIENT  DOSEAGE                                                                                              */
    /*  0   1.0      A1     40.0                                                                                              */
    /*  1   1.0      A2     40.0                                                                                              */
    /*  2   1.0      A3     20.0                                                                                              */
    /*  3   1.0      B1     20.0                                                                                              */
    /*  4   1.0      B2     10.0                                                                                              */
    /*  ..  ...     ...      ...                                                                                              */
    /*  58  7.0      B2     70.0                                                                                              */
    /*  59  7.0      B3     73.0                                                                                              */
    /*  60  7.0      C1     83.0                                                                                              */
    /*  61  7.0      C2     80.0                                                                                              */
    /*  62  7.0      C3     88.0                                                                                              */
    /*                                                                                                                        */
    /*  [63 rows x 3 columns]                                                                                                 */
    /*                                                                                                                        */
    /*                                                                                                                        */
    /* Obs    TIMEPOINT    PATIENT    DOSEAGE                                                                                 */
    /*                                                                                                                        */
    /*   1        1          A1          40                                                                                   */
    /*   2        1          A2          40                                                                                   */
    /*   3        1          A3          20                                                                                   */
    /*   4        1          B1          20                                                                                   */
    /*   5        1          B2          10                                                                                   */
    /*   6        1          B3          14                                                                                   */
    /*   7        1          C1          25                                                                                   */
    /*   8        1          C2          24                                                                                   */
    /*   9        1          C3          28                                                                                   */
    /*  10        2          A1          50                                                                                   */
    /*  11        2          A2          50                                                                                   */
    /*  12        2          A3          40                                                                                   */
    /*  13        2          B1          40                                                                                   */
    /*  14        2          B2          20                                                                                   */
    /*  15        2          B3          22                                                                                   */
    /*  16        2          C1          30                                                                                   */
    /*  17        2          C2          30                                                                                   */
    /*  18        2          C3          33                                                                                   */
    /*                                                                                                                        */
    /**************************************************************************************************************************/








    /*___ _____     _____ _____  __        _____ ____  _____
    |  _ \_ _\ \   / / _ \_   _| \ \      / /_ _|  _ \| ____|
    | |_) | | \ \ / / | | || |    \ \ /\ / / | || | | |  _|
    |  __/| |  \ V /| |_| || |     \ V  V /  | || |_| | |___
    |_|  |___|  \_/  \___/ |_|      \_/\_/  |___|____/|_____|

    */

    PROLEM

    /**************************************************************************************************************************/
    /*                       |                          |                                                                     */
    /*  INPUT                |                          |  OTPUT                                                              */
    /*                       |                          |                                                                     */
    /*  SD1.HAVE total obs=5 | 1 create SQL partitions  |  2 uses PARTITION                                                   */
    /*                       |                          |  for column names                                                   */
    /*                       |                          |                                                                     */
    /*   NAM    SCORE        | NAM  SCORE  PARTITION    |  NAM score1 score2 score3                                           */
    /*                       |                          |                                                                     */
    /*    A      0.30        |  A   0.30      1         |   A   0.30   0.45     NA                                            */
    /*    A      0.45        |  A   0.45      2         |   B   0.34  12.00     67                                            */
    /*    B      0.34        |  B   0.34      1         |                                                                     */
    /*    B     12.00        |  B  12.00      2         |                                                                     */
    /*    B     67.00        |  B  67.00      3         |                                                                     */
    /*                       |                          |                                                                     */
    /**************************************************************************************************************************/

    /*                  _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */
    options validvarname=upcase;
    libname sd1 "d:/sd1";
    data sd1.havLon;
      input  nam $1. score;
    cards4;
    A 0.30
    A 0.45
    B 0.34
    B 12.00
    B 67.00
    ;;;;
    run;quit;

     /**************************************************************************************************************************/
     /*                                                                                                                        */
     /* SD1.HAVLON total obs=5                                                                                                 */
     /*                                                                                                                        */
     /*  Obs    NAM    SCORE                                                                                                   */
     /*                                                                                                                        */
     /*   1      A      0.30                                                                                                   */
     /*   2      A      0.45                                                                                                   */
     /*   3      B      0.34                                                                                                   */
     /*   4      B     12.00                                                                                                   */
     /*   5      B     67.00                                                                                                   */
     /*                                                                                                                        */
     /**************************************************************************************************************************/

    /*                                  _       _                             _
    / | __      ___ __  ___   ___  __ _| |   __| |_   _ _ __   __ _ _ __ ___ (_) ___
    | | \ \ /\ / / `_ \/ __| / __|/ _` | |  / _` | | | | `_ \ / _` | `_ ` _ \| |/ __|
    | |  \ V  V /| |_) \__ \ \__ \ (_| | | | (_| | |_| | | | | (_| | | | | | | | (__
    |_|   \_/\_/ | .__/|___/ |___/\__, |_|  \__,_|\__, |_| |_|\__,_|_| |_| |_|_|\___|
                 |_|                 |_|          |___/
    */

    /*----                                                                   ----*/
    /*----  Get then number of columns across                                ----*/
    /*----                                                                   ----*/

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    proc sql; select count(distinct nam) into :_mx from sd1.havLon; quit;

    %utl_submit_wps64x("
    libname sd1 'd:/sd1';
    options validvarname=any;
    %array(_co,values=1-&_mx);
    proc sql;
      create
           table sd1.want as
      select
          nam
         ,%do_over(_co,phrase=%str(max(case when partition=? then score else . end) as score?) ,between=comma)
      from
          %sqlpartition(sd1.havlon,by=nam)
      group
          by nam
    ;quit;
    proc print data=sd1.want;
    run;quit;
    ");

    proc print data=sd1.want;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* SD1.WANT total obs=2                                                                                                   */
    /*                                                                                                                        */
    /* Obs    NAM    SCORE1    SCORE2                                                                                         */
    /*                                                                                                                        */
    /*  1      A      0.30       0.45                                                                                         */
    /*  2      B      0.34      12.00                                                                                         */
    /*                                                                                                                        */
    /**************************************************************************************************************************/


    /*___                                   _              _                             _
    |___ \  __      ___ __  ___   ___  __ _| |  _ __   __| |_   _ _ __   __ _ _ __ ___ (_) ___
      __) | \ \ /\ / / `_ \/ __| / __|/ _` | | | `__| / _` | | | | `_ \ / _` | `_ ` _ \| |/ __|
     / __/   \ V  V /| |_) \__ \ \__ \ (_| | | | |   | (_| | |_| | | | | (_| | | | | | | | (__
    |_____|   \_/\_/ | .__/|___/ |___/\__, |_| |_|    \__,_|\__, |_| |_|\__,_|_| |_| |_|_|\___|
                     |_|                 |_|                |___/
    */


    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    proc sql; select count(distinct nam) into :_mx from sd1.havLon; quit;

    %utl_submit_wps64x("
    libname sd1 'd:/sd1';
    options validvarname=any;
    %array(_co,values=1-&_mx);
    proc r;
    export data=sd1.havlon r=havlon;
    submit;
    library(sqldf);
    want<-sqldf('
      select
          nam
         ,%do_over(_co,phrase=%str(max(case when partition=? then score else NULL end) as score?) ,between=comma)
      from
          (select nam, score, row_number() OVER (PARTITION BY nam) as partition from havlon )
      group
          by nam
    ');
    want;
    endsubmit;
    import data=sd1.want r=want;
    run;quit;
    proc print data=sd1.want;
    run;quit;
    ");

    proc print data=sd1.want;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The WPS R System                                                                                                       */
    /*                                                                                                                        */
    /*   nam score1 score2                                                                                                    */
    /* 1   A   0.30   0.45                                                                                                    */
    /* 2   B   0.34  12.00                                                                                                    */
    /*                                                                                                                        */
    /*                                                                                                                        */
    /* The WPS R System                                                                                                       */
    /*                                                                                                                        */
    /* Obs    nam    score1    score2                                                                                         */
    /*                                                                                                                        */
    /*  1      A       .30       0.45                                                                                         */
    /*  2      B       .34      12.00                                                                                         */
    /*                                                                                                                        */
    /**************************************************************************************************************************/


    /*____                                    _   _                             _       _                             _
    |___ /  __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |   __| |_   _ _ __   __ _ _ __ ___ (_) ___
      |_ \  \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |  / _` | | | | `_ \ / _` | `_ ` _ \| |/ __|
     ___) |  \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | | | (_| | |_| | | | | (_| | | | | | | | (__
    |____/    \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|  \__,_|\__, |_| |_|\__,_|_| |_| |_|_|\___|
                     |_|         |_|    |___/                                |_|          |___/
    */

    proc datasets lib=sd1 nolist mt=data mt=view nodetails;delete want; run;quit;

    %utlfkil(d:/xpt/want.xpt);

    proc sql; select count(distinct nam) into :_mx from sd1.havLon; quit;

    %utlfkil(d:/xpt/want.xpt);

    %utl_submit_wps64x("
    options validvarname=any lrecl=32756;
    libname sd1 'd:/sd1';
    %array(_co,values=1-&_mx);
    proc python;
    export data=sd1.havlon python=havlon;
    submit;
    import pyreadstat;
    from os import path;
    import pandas as pd;
    import numpy as np;
    import pandas as pd;
    from pandasql import sqldf;
    mysql = lambda q: sqldf(q, globals());
    from pandasql import PandaSQL;
    pdsql = PandaSQL(persist=True);
    sqlite3conn = next(pdsql.conn.gen).connection.connection;
    sqlite3conn.enable_load_extension(True);
    sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
    mysql = lambda q: sqldf(q, globals());
    want = pdsql('''
      select
          nam
         ,%do_over(_co,phrase=%str(max(case when partition=? then score else NULL end) as score?) ,between=comma)
      from
          (select nam, score, row_number() OVER (PARTITION BY nam) as partition from havlon )
      group
          by nam
    ''');
    print(want);
    pyreadstat.write_xport(want, 'd:/xpt/want.xpt',table_name='want',file_format_version=5
    ,column_labels=['TIMEPOINT', 'PATIENT', 'DOSEAGE' ]);
    endsubmit;
    run;quit;
    ");

    /*--- handles long variable names by using the label to rename the variables  ----*/

    libname xpt xport "d:/xpt/want.xpt";
    proc contents data=xpt._all_;
    run;quit;

    data want_pivot_wide;
      %utl_rens(xpt.want) ;
      set want;
    run;quit;
    libname xpt clear;

    proc print data=want_pivot_wide;
    run;quit;


    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The WPS PYTHON Procedure                                                                                               */
    /*                                                                                                                        */
    /*   nam  score1  score2                                                                                                  */
    /* 0   A    0.30    0.45                                                                                                  */
    /* 1   B    0.34   12.00                                                                                                  */
    /*                                                                                                                        */
    /* The WPS System                                                                                                         */
    /*                                                                                                                        */
    /* Obs    TIMEPOINT    PATIENT    DOSEAGE                                                                                 */
    /*                                                                                                                        */
    /*  1         A          0.30       0.45                                                                                  */
    /*  2         B          0.34      12.00                                                                                  */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

     /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */

