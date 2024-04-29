-- Cargar los datos
data = LOAD '/tmp/data/dataset.csv' USING PigStorage(',') AS (
    Type: chararray,
    Days_for_shipping_real: int,
    Days_for_shipment_scheduled: int,
    Benefit_per_order: float,
    Sales_per_customer: float,
    Delivery_Status: chararray,
    Late_delivery_risk: int,
    Category_Id: int,
    Category_Name: chararray,
    Customer_City: chararray,
    Customer_Country: chararray,
    Customer_Fname: chararray,
    Customer_Id: int,
    Customer_Lname: chararray,
    Customer_Segment: chararray,
    Customer_State: chararray,
    Customer_Street: chararray,
    Customer_Zipcode: chararray,
    Department_Id: int,
    Department_Name: chararray,
    Market: chararray,
    Order_City: chararray,
    Order_Country: chararray,
    Order_Customer_Id: int,
    Order_Date: chararray,
    Order_Id: int,
    Order_Item_Cardprod_Id: int,
    Order_Item_Discount: float,
    Order_Item_Discount_Rate: float,
    Order_Item_Id: int,
    Order_Item_Product_Price: float,
    Order_Item_Profit_Ratio: float,
    Order_Item_Quantity: int,
    Sales: float,
    Order_Item_Total: float,
    Order_Profit_Per_Order: float,
    Order_Region: chararray,
    Order_State: chararray,
    Order_Status: chararray,
    Order_Zipcode: chararray,
    Product_Card_Id: int,
    Product_Category_Id: int,
    Product_Name: chararray,
    Product_Price: float,
    Product_Status: chararray,
    Shipping_Date: chararray,
    Shipping_Mode: chararray
);

-- Contador del numero de filas, excluyendo la cabecera
num_rows = FOREACH (FILTER data BY NOT $0 MATCHES 'Type') GENERATE $0;
num_rows_grouped = GROUP num_rows ALL;
row_count = FOREACH num_rows_grouped GENERATE COUNT(num_rows);

DUMP row_count;
