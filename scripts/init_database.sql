/* three schemas of DataWareHouse
running this script will drop the entire DataWareHouse database, if exists.
*/
use master;

-- drop and recreate DataWareHouse database if exists

create database DataWareHouse;

use DataWareHouse;

create schema bronze;
GO
create schema silver;
GO
create schema gold;
GO

