-- Напишите запросы, которые выводят следующую информацию:
-- 1. Название компании заказчика (company_name из табл. customers) и ФИО сотрудника, работающего над заказом этой компании (см таблицу employees),
-- когда и заказчик и сотрудник зарегистрированы в городе London, а доставку заказа ведет компания United Package (company_name в табл shippers)
SELECT customer.company_name, CONCAT(employee.first_name, ' ', employee.last_name)
FROM orders AS fullname
JOIN customers AS customer USING(customer_id)
JOIN employees AS employee USING(employee_id)
JOIN shippers AS shipper ON fullname.ship_via = shipper.shipper_id
WHERE customer.city = 'London' AND employee.city = 'London' AND shipper.company_name = 'United Package';


-- 2. Наименование продукта, количество товара (product_name и units_in_stock в табл products),
-- имя поставщика и его телефон (contact_name и phone в табл suppliers) для таких продуктов,
-- которые не сняты с продажи (поле discontinued) и которых меньше 25 и которые в категориях Dairy Products и Condiments.
-- Отсортировать результат по возрастанию количества оставшегося товара.
SELECT product_name, units_in_stock, contact_name, phone
FROM products
JOIN categories USING(category_id)
JOIN suppliers USING(supplier_id)
WHERE discontinued != 1 AND units_in_stock < 25 AND category_name IN ('Dairy Products', 'Condiments')
ORDER BY units_in_stock;

-- 3. Список компаний заказчиков (company_name из табл customers), не сделавших ни одного заказа
SELECT company_name
FROM customers
LEFT JOIN orders USING(customer_id)
WHERE order_id IS NULL;

-- 4. уникальные названия продуктов, которых заказано ровно 10 единиц (количество заказанных единиц см в колонке quantity табл order_details)
-- Этот запрос написать именно с использованием подзапроса.
