create table warehouse (
w_id smallint not null,
w_name varchar(10), 
w_street_1 varchar(20), 
w_street_2 varchar(20), 
w_city varchar(20), 
w_state varchar(2), 
w_zip varchar(9), 
w_tax decimal(4,2), 
w_ytd decimal(12,2),
tenant_id int not null,
is_insert int,
is_update int
);

create table district (
d_id tinyint not null, 
d_w_id smallint not null, 
d_name varchar(10), 
d_street_1 varchar(20), 
d_street_2 varchar(20), 
d_city varchar(20), 
d_state varchar(2), 
d_zip varchar(9), 
d_tax decimal(4,2), 
d_ytd decimal(12,2), 
d_next_o_id int,
tenant_id int not null,
is_insert int,
is_update int
 );

create table customer (
c_id int not null, 
c_d_id tinyint not null,
c_w_id smallint not null, 
c_first varchar(16), 
c_middle varchar(2), 
c_last varchar(16), 
c_street_1 varchar(20), 
c_street_2 varchar(20), 
c_city varchar(20), 
c_state varchar(2), 
c_zip varchar(9), 
c_phone varchar(16), 
c_since TIMESTAMP, 
c_credit varchar(2), 
c_credit_lim bigint, 
c_discount decimal(4,2), 
c_balance decimal(12,2), 
c_ytd_payment decimal(12,2), 
c_payment_cnt smallint, 
c_delivery_cnt smallint, 
c_data varchar(500),
tenant_id int not null,
is_insert int,
is_update int
 ) ;

create table history (
h_c_id int, 
h_c_d_id tinyint, 
h_c_w_id smallint,
h_d_id tinyint,
h_w_id smallint,
h_date TIMESTAMP,
h_amount decimal(6,2),
h_data varchar(24),
tenant_id int not null,
is_insert int,
is_update int );

create table new_orders (
no_o_id int not null,
no_d_id tinyint not null,
no_w_id smallint not null,
tenant_id int not null,
is_insert int,
is_update int
);

create table orders (
o_id int not null, 
o_d_id tinyint not null, 
o_w_id smallint not null,
o_c_id int,
o_entry_d TIMESTAMP,
o_carrier_id tinyint,
o_ol_cnt tinyint, 
o_all_local tinyint,
tenant_id int not null,
is_insert int,
is_update int
) ;

create table order_line ( 
ol_o_id int not null, 
ol_d_id tinyint not null,
ol_w_id smallint not null,
ol_number tinyint not null,
ol_i_id int, 
ol_supply_w_id smallint,
ol_delivery_d TIMESTAMP, 
ol_quantity tinyint, 
ol_amount decimal(6,2), 
ol_dist_info varchar(24),
tenant_id int not null,
is_insert int,
is_update int
 );

create table item (
i_id int not null, 
i_im_id int, 
i_name varchar(24), 
i_price decimal(5,2), 
i_data varchar(50),
tenant_id int not null,
is_insert int,
is_update int
 );

create table stock (
s_i_id int not null, 
s_w_id smallint not null, 
s_quantity smallint, 
s_dist_01 varchar(24), 
s_dist_02 varchar(24),
s_dist_03 varchar(24),
s_dist_04 varchar(24), 
s_dist_05 varchar(24), 
s_dist_06 varchar(24), 
s_dist_07 varchar(24), 
s_dist_08 varchar(24), 
s_dist_09 varchar(24), 
s_dist_10 varchar(24), 
s_ytd decimal(8,0), 
s_order_cnt smallint, 
s_remote_cnt smallint,
s_data varchar(50),
tenant_id int not null,
is_insert int,
is_update int
);

CREATE INDEX cIndex ON customer (c_w_id, c_d_id, tenant_id);
CREATE INDEX dIndex ON district (d_w_id, d_id, tenant_id);
CREATE INDEX iIndex ON item (i_id, tenant_id);
CREATE INDEX noIndex ON new_orders (no_w_id, no_d_id, no_o_id, tenant_id);
CREATE INDEX oIndex ON orders (o_w_id, o_d_id, o_id, tenant_id);
CREATE INDEX olIndex ON order_line (ol_w_id, ol_d_id, ol_o_id, tenant_id);
CREATE INDEX sIndex ON stock (s_w_id, s_i_id, tenant_id);
CREATE INDEX wIndex ON warehouse (w_id, tenant_id);

PARTITION TABLE warehouse ON COLUMN tenant_id;
PARTITION TABLE district ON COLUMN tenant_id;
PARTITION TABLE customer ON COLUMN tenant_id;
PARTITION TABLE history ON COLUMN tenant_id;
PARTITION TABLE new_orders ON COLUMN tenant_id;
PARTITION TABLE orders ON COLUMN tenant_id;
PARTITION TABLE order_line ON COLUMN tenant_id;
PARTITION TABLE item ON COLUMN tenant_id;
PARTITION TABLE stock ON COLUMN tenant_id;

CREATE PROCEDURE FROM CLASS Procedure0;
CREATE PROCEDURE FROM CLASS Procedure1;
CREATE PROCEDURE FROM CLASS Procedure2;
CREATE PROCEDURE FROM CLASS Procedure3;
CREATE PROCEDURE FROM CLASS Procedure4;
CREATE PROCEDURE FROM CLASS Procedure5;
CREATE PROCEDURE FROM CLASS Procedure6;
CREATE PROCEDURE FROM CLASS Procedure7;
CREATE PROCEDURE FROM CLASS Procedure8;
CREATE PROCEDURE FROM CLASS Procedure9;
CREATE PROCEDURE FROM CLASS Procedure10;
CREATE PROCEDURE FROM CLASS Procedure11;
CREATE PROCEDURE FROM CLASS Procedure12;
CREATE PROCEDURE FROM CLASS Procedure13;
CREATE PROCEDURE FROM CLASS Procedure14;
CREATE PROCEDURE FROM CLASS Procedure15;
CREATE PROCEDURE FROM CLASS Procedure16;
CREATE PROCEDURE FROM CLASS Procedure17;
CREATE PROCEDURE FROM CLASS Procedure18;
CREATE PROCEDURE FROM CLASS Procedure19;
CREATE PROCEDURE FROM CLASS Procedure20;
CREATE PROCEDURE FROM CLASS Procedure21;
CREATE PROCEDURE FROM CLASS Procedure22;
CREATE PROCEDURE FROM CLASS Procedure23;
CREATE PROCEDURE FROM CLASS Procedure24;
CREATE PROCEDURE FROM CLASS Procedure25;
CREATE PROCEDURE FROM CLASS Procedure26;
CREATE PROCEDURE FROM CLASS Procedure27;
CREATE PROCEDURE FROM CLASS Procedure28;
CREATE PROCEDURE FROM CLASS Procedure29;
CREATE PROCEDURE FROM CLASS Procedure30;
CREATE PROCEDURE FROM CLASS Procedure31;
CREATE PROCEDURE FROM CLASS Procedure32;
CREATE PROCEDURE FROM CLASS Procedure33;
CREATE PROCEDURE FROM CLASS Procedure34;
CREATE PROCEDURE FROM CLASS ProcedureSelectCustomer;
CREATE PROCEDURE FROM CLASS ProcedureSelectDistrict;
CREATE PROCEDURE FROM CLASS ProcedureSelectOrderLine;
CREATE PROCEDURE FROM CLASS ProcedureSelectOrders;
CREATE PROCEDURE FROM CLASS ProcedureSelectStock;
CREATE PROCEDURE FROM CLASS ProcedureSelectWarehouse;
CREATE PROCEDURE FROM CLASS ProcedureInsertCustomer;
CREATE PROCEDURE FROM CLASS ProcedureInsertDistrict;
CREATE PROCEDURE FROM CLASS ProcedureInsertOrderLine;
CREATE PROCEDURE FROM CLASS ProcedureInsertOrders;
CREATE PROCEDURE FROM CLASS ProcedureInsertStock;
CREATE PROCEDURE FROM CLASS ProcedureInsertWarehouse;
CREATE PROCEDURE FROM CLASS PRSelectAll;
CREATE PROCEDURE FROM CLASS PRTruncateAll;
CREATE PROCEDURE FROM CLASS PRSelectAllCustomer;
CREATE PROCEDURE FROM CLASS PRSelectAllDistrict;
CREATE PROCEDURE FROM CLASS PRSelectAllHistory;
CREATE PROCEDURE FROM CLASS PRSelectAllItem;
CREATE PROCEDURE FROM CLASS PRSelectAllNewOrders;
CREATE PROCEDURE FROM CLASS PRSelectAllOrderLine;
CREATE PROCEDURE FROM CLASS PRSelectAllOrders;
CREATE PROCEDURE FROM CLASS PRSelectAllStock;
CREATE PROCEDURE FROM CLASS PRSelectAllWarehouse;
CREATE PROCEDURE FROM CLASS PRDeleteAllCustomer;
CREATE PROCEDURE FROM CLASS PRDeleteAllDistrict;
CREATE PROCEDURE FROM CLASS PRDeleteAllHistory;
CREATE PROCEDURE FROM CLASS PRDeleteAllItem;
CREATE PROCEDURE FROM CLASS PRDeleteAllNewOrders;
CREATE PROCEDURE FROM CLASS PRDeleteAllOrderLine;
CREATE PROCEDURE FROM CLASS PRDeleteAllOrders;
CREATE PROCEDURE FROM CLASS PRDeleteAllStock;
CREATE PROCEDURE FROM CLASS PRDeleteAllWarehouse;
CREATE PROCEDURE FROM CLASS ProcedureInsertHistory;
CREATE PROCEDURE FROM CLASS ProcedureInsertNewOrders;
CREATE PROCEDURE FROM CLASS ProcedureInsertItem;

PARTITION PROCEDURE Procedure0 ON TABLE district COLUMN tenant_id;
PARTITION PROCEDURE Procedure1 ON TABLE item COLUMN tenant_id;
PARTITION PROCEDURE Procedure2 ON TABLE stock COLUMN tenant_id;
PARTITION PROCEDURE Procedure3 ON TABLE warehouse COLUMN tenant_id;
PARTITION PROCEDURE Procedure4 ON TABLE district COLUMN tenant_id;
PARTITION PROCEDURE Procedure5 ON TABLE customer COLUMN tenant_id;
PARTITION PROCEDURE Procedure6 ON TABLE customer COLUMN tenant_id;
PARTITION PROCEDURE Procedure7 ON TABLE customer COLUMN tenant_id;
PARTITION PROCEDURE Procedure8 ON TABLE customer COLUMN tenant_id;
PARTITION PROCEDURE Procedure9 ON TABLE customer COLUMN tenant_id;
PARTITION PROCEDURE Procedure10 ON TABLE customer COLUMN tenant_id;
PARTITION PROCEDURE Procedure11 ON TABLE customer COLUMN tenant_id;
PARTITION PROCEDURE Procedure12 ON TABLE order_line COLUMN tenant_id;
PARTITION PROCEDURE Procedure13 ON TABLE new_orders COLUMN tenant_id;
PARTITION PROCEDURE Procedure14 ON TABLE orders COLUMN tenant_id;
PARTITION PROCEDURE Procedure15 ON TABLE order_line COLUMN tenant_id;
PARTITION PROCEDURE Procedure16 ON TABLE district COLUMN tenant_id;
PARTITION PROCEDURE Procedure17 ON TABLE order_line COLUMN tenant_id;
PARTITION PROCEDURE Procedure18 ON TABLE stock COLUMN tenant_id;

PARTITION PROCEDURE Procedure20 ON TABLE orders COLUMN tenant_id;

PARTITION PROCEDURE Procedure21 ON TABLE orders COLUMN tenant_id;
PARTITION PROCEDURE Procedure22 ON TABLE new_orders COLUMN tenant_id;
PARTITION PROCEDURE Procedure23 ON TABLE order_line COLUMN tenant_id;
PARTITION PROCEDURE Procedure24 ON TABLE history COLUMN tenant_id;
PARTITION PROCEDURE Procedure25 ON TABLE district COLUMN tenant_id;
PARTITION PROCEDURE Procedure26 ON TABLE stock COLUMN tenant_id;
PARTITION PROCEDURE Procedure27 ON TABLE warehouse COLUMN tenant_id;
PARTITION PROCEDURE Procedure28 ON TABLE district COLUMN tenant_id;
PARTITION PROCEDURE Procedure29 ON TABLE customer COLUMN tenant_id;
PARTITION PROCEDURE Procedure30 ON TABLE customer COLUMN tenant_id;
PARTITION PROCEDURE Procedure31 ON TABLE orders COLUMN tenant_id;
PARTITION PROCEDURE Procedure32 ON TABLE order_line COLUMN tenant_id;
PARTITION PROCEDURE Procedure33 ON TABLE customer COLUMN tenant_id;
PARTITION PROCEDURE Procedure34 ON TABLE new_orders COLUMN tenant_id;
PARTITION PROCEDURE ProcedureSelectCustomer ON TABLE customer COLUMN tenant_id;
PARTITION PROCEDURE ProcedureSelectDistrict ON TABLE district COLUMN tenant_id;
PARTITION PROCEDURE ProcedureSelectOrderLine ON TABLE order_line COLUMN tenant_id;
PARTITION PROCEDURE ProcedureSelectOrders ON TABLE orders COLUMN tenant_id;
PARTITION PROCEDURE ProcedureSelectStock ON TABLE stock COLUMN tenant_id;
PARTITION PROCEDURE ProcedureSelectWarehouse ON TABLE warehouse COLUMN tenant_id;
PARTITION PROCEDURE ProcedureInsertCustomer ON TABLE customer COLUMN tenant_id;
PARTITION PROCEDURE ProcedureInsertDistrict ON TABLE district COLUMN tenant_id;
PARTITION PROCEDURE ProcedureInsertOrderLine ON TABLE order_line COLUMN tenant_id;
PARTITION PROCEDURE ProcedureInsertOrders ON TABLE orders COLUMN tenant_id;
PARTITION PROCEDURE ProcedureInsertStock ON TABLE stock COLUMN tenant_id;
PARTITION PROCEDURE ProcedureInsertWarehouse ON TABLE warehouse COLUMN tenant_id;

PARTITION PROCEDURE PRSelectAllCustomer ON TABLE customer COLUMN tenant_id;
PARTITION PROCEDURE PRSelectAllDistrict ON TABLE district COLUMN tenant_id;
PARTITION PROCEDURE PRSelectAllHistory ON TABLE history COLUMN tenant_id;
PARTITION PROCEDURE PRSelectAllItem ON TABLE item COLUMN tenant_id;
PARTITION PROCEDURE PRSelectAllNewOrders ON TABLE new_orders COLUMN tenant_id;
PARTITION PROCEDURE PRSelectAllOrderLine ON TABLE order_line COLUMN tenant_id;
PARTITION PROCEDURE PRSelectAllOrders ON TABLE orders COLUMN tenant_id;
PARTITION PROCEDURE PRSelectAllStock ON TABLE stock COLUMN tenant_id;
PARTITION PROCEDURE PRSelectAllWarehouse ON TABLE warehouse COLUMN tenant_id;

PARTITION PROCEDURE PRDeleteAllCustomer ON TABLE customer COLUMN tenant_id;
PARTITION PROCEDURE PRDeleteAllDistrict ON TABLE district COLUMN tenant_id;
PARTITION PROCEDURE PRDeleteAllHistory ON TABLE history COLUMN tenant_id;
PARTITION PROCEDURE PRDeleteAllItem ON TABLE item COLUMN tenant_id;
PARTITION PROCEDURE PRDeleteAllNewOrders ON TABLE new_orders COLUMN tenant_id;
PARTITION PROCEDURE PRDeleteAllOrderLine ON TABLE order_line COLUMN tenant_id;
PARTITION PROCEDURE PRDeleteAllOrders ON TABLE orders COLUMN tenant_id;
PARTITION PROCEDURE PRDeleteAllStock ON TABLE stock COLUMN tenant_id;
PARTITION PROCEDURE PRDeleteAllWarehouse ON TABLE warehouse COLUMN tenant_id;

PARTITION PROCEDURE ProcedureInsertHistory ON TABLE history COLUMN tenant_id;
PARTITION PROCEDURE ProcedureInsertNewOrders ON TABLE new_orders COLUMN tenant_id;
PARTITION PROCEDURE ProcedureInsertItem ON TABLE item COLUMN tenant_id;
