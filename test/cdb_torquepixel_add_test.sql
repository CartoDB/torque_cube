SELECT '1', CDB_TorquePixel_add('{0,1,1,1}','{0,1,2,3}');
SELECT '2', CDB_TorquePixel_add('{0,1,1,1}','{1,9,1,2,3}');
SELECT '3', CDB_TorquePixel_add('{0,1,1,1}','{2,10,11,1,2,3}');
SELECT '4', CDB_TorquePixel_add('{1,9,1,1,1}','{1,9,1,2,3}');
SELECT '5', CDB_TorquePixel_add('{2,8,9,1,2,1,2,1,2}','{1,9,1,2,3}');
SELECT '6', CDB_TorquePixel_add('{1,8,1,2,3}','{1,3,9,8,7}');
SELECT '7', CDB_TorquePixel_add('{2,8,9,1,2,1,2,1,2}','{1,3,9,8,7}');
SELECT '8', CDB_TorquePixel_add(NULL,'{1,3,9,8,7}');
