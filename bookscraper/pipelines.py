# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class BookscraperPipeline:
    def process_item(self, item, spider):

        adapter = ItemAdapter(item)

        ## Strip all whitespaces from strings
        field_names = adapter.field_names()
        for field_name in field_names:
                if field_name != "description":
                    value = adapter.get(field_name)
                    
                    print(f"{field_name}: {value}")
                    
                    adapter[field_name] = value[0].strip()

        ## Category & Product Type --> switch to lowercase
        lowercase_keys = ["category", "product_type"]
        for lowercase_key in lowercase_keys:
                if lowercase_key != "description":
                     value = adapter.get(lowercase_key)
                     adapter[lowercase_key] = value.lower()

        ## Price --> convert to float
        price_keys = ["price", "price_excl_tax", "price_incl_tax", "tax"]
        for price_key in price_keys:
            value = adapter.get(price_key)
            value = value.replace("£", "")
            print("***************")
            print("Tax value before conversion:", value)
            print("***************")
            adapter[price_key] = float(value)

        availability_string = adapter.get("availability")
        split_string_array = availability_string.split("(")
        if len(split_string_array) < 2:
             adapter["availability"] = 0
        else:
             availability_array = split_string_array[1].split(" ")
             adapter["availability"] = int(availability_array[0])

        ## Reviews --> convert string to number
        num_reviews_string = adapter.get("num_reviews")
        adapter["num_reviews"] = int(num_reviews_string)

        ## Stars --> convert text to number
        stars_string = adapter.get("stars")
        split_stars_array = stars_string.split(" ")
        stars_text_value = split_stars_array[1].lower()
        if stars_text_value == "zero":
            adapter["stars"] = 0
        elif stars_text_value == "one":
            adapter["stars"] = 1
        elif stars_text_value == "two":
            adapter["stars"] = 2
        elif stars_text_value == "three":
            adapter["stars"] = 3
        elif stars_text_value == "four":
            adapter["stars"] = 4
        elif stars_text_value == "five":
            adapter["stars"] = 5


        return item
    
import mysql.connector

class SaveToMySQLpipeline:
    def __init__(self):
        self.conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="jacquet1453",
            database="test"
        )
        self.cur = self.conn.cursor()
        self.create_table()

    def create_table(self):
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS test(
               id int NOT NULL auto_increment,
               url VARCHAR(255),
               title text,
               upc VARCHAR(255),
               product_type VARCHAR(255),
               price_excl_tax DECIMAL,
               price_incl_tax DECIMAL,
               tax DECIMAL,
               availability INTEGER,
               num_reviews INTEGER,
               stars INTEGER,
               category VARCHAR(255),
               price DECIMAL,
               description text,
               PRIMARY KEY (id)
            )
        """)

    def process_item(self, item, spider):

        print(item.keys())
        try:
            self.cur.execute(
                 """
                INSERT INTO test(
                    url, title, upc, product_type, price_excl_tax, price_incl_tax,
                    tax, availability, num_reviews, stars, category, price, description
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )""",
                (
                    item["url"], item["title"], item["upc"], item["product_type"],
                    item["price_excl_tax"], item["price_incl_tax"], item["tax"],
                    item["availability"], item["num_reviews"], item["stars"],
                    item["category"], item["price"], str(item["description"][0])
                )
            )
            self.conn.commit()
        except Exception as e:
            # Log the error or handle it as appropriate
            print("Error:", e)
            self.conn.rollback()

        return item

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()