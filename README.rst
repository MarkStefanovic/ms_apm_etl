========
sqlnomad
========


Monadic wrapper around SQL syntax


Installation
============

The package can be installed with pip using the following command:

.. code-block:: bash

    $ pip install sqlnomad


Description
===========

SQL is verbose, and the differences between SQL dialects make migration between databases tedious.  This project aims to solve both issues by abstracting SQL syntax into a series of subquery primitives and providing monadic functions to transform them.


Examples
========

.. code:: python

    >>> from sqlnomad import *

    >>> customers = StorageTable(
    ...     storage_name="customer",
    ...     display_name="Customers",
    ...     fields=[
    ...         StorageField("id", "Customer ID"),
    ...         StorageField("first_name", "First Name"),
    ...         StorageField("last_name", "Last Name")
    ...     ],
    ...     dialect=SqlDialect.MSSQL,
    ...     schema="dim"
    ... )

    >>> sales = StorageTable(
    ...     storage_name="sales",
    ...     display_name="Sales",
    ...     fields=[
    ...         StorageField("id", "Sales ID"),
    ...         StorageField("customer_id", "Customer ID"),
    ...         StorageField("sales_date", "Sales Date"),
    ...         StorageField("sales_amount", "Sales Amount")
    ...     ],
    ...     dialect=SqlDialect.MSSQL,
    ...     schema="fact"
    ... )

We can combine storage tables using the join function

.. code:: python

    >>> transactions = Query(customers).bind(
    ...     join(
    ...         right_table=sales,
    ...         left_field_alias="Customer ID",
    ...         right_field_alias="Customer ID"
    ...     )
    ... )

Aggregations are performed with the aggregate function.
Let's also sort the results by the total Sales Amount in descending order.

.. code:: python

    >>> total_sales = transactions.bind(
    ...     aggregate(
    ...         group_by_fields=["First Name", "Last Name"],
    ...         aggregations={"Sales Amount": AggregationMethod.SUM}
    ...     )
    ... ).bind(order_by({"Sales Amount": SortOrder.DESCENDING}))
    >>> total_sales.sql

This generates the following query:

.. code:: SQL

    SELECT [Customers 3].[First Name] AS [First Name],
           [Customers 3].[Last Name] AS [Last Name],
           [Customers 3].[Sales Amount] AS [Sales Amount]
    FROM (
      SELECT [Customers 2].[First Name] AS [First Name],
             [Customers 2].[Last Name] AS [Last Name],
             SUM([Customers 2].[Sales Amount]) AS [Sales Amount]
      FROM (
          SELECT [Customers].[Customer ID] AS [Customer ID],
                 [Customers].[First Name] AS [First Name],
                 [Customers].[Last Name] AS [Last Name],
                 [Sales].[Sales Amount] AS [Sales Amount],
                 [Sales].[Sales Date] AS [Sales Date],
                 [Sales].[Sales ID] AS [Sales ID]
          FROM (
              SELECT [id] AS [Customer ID],
                     [first_name] AS [First Name],
                     [last_name] AS [Last Name]
              FROM [dim].[customer]
          ) [Customers]
          INNER JOIN (
              SELECT [id] AS [Sales ID],
                     [customer_id] AS [Customer ID],
                     [sales_date] AS [Sales Date],
                     [sales_amount] AS [Sales Amount]
              FROM [fact].[sales]
          ) [Sales]
          ON [Customers].[Customer ID] = [Sales].[Customer ID]
      ) [Customers 2]
      GROUP BY [Customers 2].[First Name],
               [Customers 2].[Last Name]
    ) [Customers 3]
    ORDER BY [Customers 3].[Sales Amount] DESC

We can also combine storage tables with raw sql Subquery objects, for example:

.. code:: python

    >>> sql = """
    ...     SELECT c.id AS [Customer ID]
    ...     FROM dim.customer AS c
    ...     JOIN fact.sales AS s
    ...       ON c.id = s.customer_id
    ...     GROUP BY c.id
    ...     HAVING SUM(sales_amount) >= 10000
    ...  """
    >>> whales = Subquery(
    ...    sql=sql,
    ...    fields=[SqlField(alias="Customer ID", definition="Customer ID")],
    ...    dialect=SqlDialect.MSSQL,
    ...    alias="Whales"
    ... )
    >>> qry = Query(whales).bind(
    ...     join(
    ...         right_table=transactions,
    ...         left_field_alias="Customer ID",
    ...         right_field_alias="Customer ID"
    ...     )
    ... )

We can also filter our queries using the find function:

.. code:: python

    >>> and_or_filter = transactions.bind(
    ...     find([
    ...         [SqlFilter("First Name", FilterOperator.STARTS_WITH, "Steve"),
    ...          SqlFilter("Last Name", FilterOperator.EQUALS, "Smith")],
    ...         [SqlFilter("Sales Amount", FilterOperator.GREATER_THAN, 10)]
    ...     ])
    ... )


Note
====

This project is in its infancy.  The API will be subject to drastic changes up until its 1.0 release.
