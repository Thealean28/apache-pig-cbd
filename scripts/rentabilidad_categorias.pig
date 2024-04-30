-- Cargar datos
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

-- Generar columnas necesarias
relevant_data = FOREACH data GENERATE Category_Name, Benefit_per_order, Sales;

-- Agrupar por categoría de producto
grouped_data = GROUP relevant_data BY Category_Name;

-- Calcular el beneficio total, las ventas totales y el margen de beneficio para cada categoría
category_profitability = FOREACH grouped_data {
    total_benefit = SUM(relevant_data.Benefit_per_order);
    total_sales = SUM(relevant_data.Sales);
    profit_margin = (total_benefit / total_sales) * 100; -- Calcular el margen de beneficio como porcentaje
    GENERATE group AS Category_Name, total_benefit AS Total_Benefit, total_sales AS Total_Sales, profit_margin AS Profit_Margin;
}

-- Ordenar por margen de beneficio de forma descendente
sorted_data = ORDER category_profitability BY Profit_Margin DESC;

-- Almacenar los resultados
STORE sorted_data INTO '/tmp/data/category_profitability' USING PigStorage(',');

