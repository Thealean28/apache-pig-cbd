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

-- Contamos el número de veces que un cliente compra un producto
customer_product_counts = FOREACH (GROUP data BY (Customer_Id, Product_Name)) {
    product_count = COUNT(data);
    GENERATE FLATTEN(group) AS (Customer_Id, Product_Name), product_count AS Product_Count;
}

-- Encontrar el producto más frecuente para cada cliente
top_customer_products = FOREACH (GROUP customer_product_counts BY Customer_Id) {
    sorted_products = ORDER customer_product_counts BY Product_Count DESC;
    top_product = LIMIT sorted_products 1;
    GENERATE FLATTEN(top_product);
}

-- Se muestran los 100 primeros por simplificar
limited_products = LIMIT top_customer_products 100;

-- Se muestran los resultados
DUMP limited_products;
