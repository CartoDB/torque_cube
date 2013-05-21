SELECT '0', CDB_TorquePixel_sum('{0,1}',0);
SELECT '1', CDB_TorquePixel_sum('{1,1,2,3}',0);
SELECT '2', CDB_TorquePixel_sum('{1,1,2,3}',1);
SELECT '3', CDB_TorquePixel_sum('{9,1,2,3,4,5,6,7,8,9,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3}',0);
SELECT '4', CDB_TorquePixel_sum('{9,1,2,3,4,5,6,7,8,9,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3}',1);
-- This is invalid, should we raise an exception instead ?
SELECT '5', CDB_TorquePixel_sum('{9,1,2,3,4,5,6,7,8,9,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3}',2);
