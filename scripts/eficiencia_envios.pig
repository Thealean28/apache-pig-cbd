datos = LOAD '/tmp/data/dataset.csv' USING PigStorage(',') AS (
    Type:chararray,
    DaysForShippingReal:int,
    DaysForShipmentScheduled:int,
    BenefitPerOrder:double,
    SalesPerCustomer:double,
    DeliveryStatus:chararray,
    LateDeliveryRisk:chararray,
    CategoryId:int,
    CategoryName:chararray,
    CustomerCity:chararray,
    CustomerCountry:chararray,
    CustomerFname:chararray,
    CustomerId:int,
    CustomerLname:chararray,
    CustomerPassword:chararray,
    CustomerSegment:chararray,
    CustomerState:chararray,
    CustomerStreet:chararray,
    CustomerZipcode:int,
    DepartmentId:int,
    DepartmentName:chararray,
    Market:chararray,
    OrderCity:chararray,
    OrderCountry:chararray,
    OrderCustomerId:int,
    OrderDate:chararray,
    OrderId:int,
    OrderItemCardprodId:int,
    OrderItemDiscount:double,
    OrderItemDiscountRate:double,
    OrderItemId:int,
    OrderItemProductPrice:double,
    OrderItemProfitRatio:double,
    OrderItemQuantity:int,
    Sales:double,
    OrderItemTotal:double,
    OrderProfitPerOrder:double,
    OrderRegion:chararray,
    OrderState:chararray,
    OrderStatus:chararray,
    OrderZipcode:int,
    ProductCardId:int,
    ProductCategoryId:int,
    ProductName:chararray,
    ProductPrice:double,
    ProductStatus:chararray,
    ShippingDate:chararray,
    ShippingMode:chararray
);   
-- Agrupar los pedidos por región
group_by_region = GROUP datos BY OrderRegion;

-- Calcular el promedio de días de envío por región
average_shipping_days_by_region = FOREACH group_by_region GENERATE
    group AS order_region,
    AVG(datos.DaysForShippingReal) AS avg_shipping_days;
    
-- Filtrar resultados debido a que hay valores nulos
filter_results = FILTER average_shipping_days_by_region BY avg_shipping_days > 0 AND avg_shipping_days IS NOT NULL;

-- Ordenar por regiones mas rapidas
sorted_by_shipping_days = ORDER filter_results BY avg_shipping_days;

-- Almacenar resultados
STORE sorted_by_shipping_days INTO '/tmp/data/avg_shipping_days' USING PigStorage(',');
