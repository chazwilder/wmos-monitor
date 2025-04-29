"""
Oracle database client module for connecting to and querying Oracle databases
"""

from utils.logger import logger

try:
    import cx_Oracle
except ImportError:
    try:
        import oracledb as cx_Oracle

        try:
            cx_Oracle.init_oracle_client()
        except:
            pass
    except ImportError:
        logger.error("Error: Neither cx_Oracle nor oracledb package is installed")
        logger.error("Please install one of them using:")
        logger.error("pip install cx_Oracle")
        logger.error("or")
        logger.error("pip install oracledb")
        raise ImportError("Oracle client libraries not available")


class OracleClient:
    """Client for connecting to and querying Oracle databases"""

    def __init__(self, connection_string):
        """
        Initialize the Oracle client

        Args:
            connection_string (str): Oracle connection string
        """
        self.connection_string = connection_string
        self.connection = None

    def connect(self):
        """
        Connect to the Oracle database

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.connection = cx_Oracle.connect(self.connection_string)
            logger.info("Connected to Oracle database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Oracle database: {str(e)}")
            return False

    def close(self):
        """Close the Oracle database connection"""
        if self.connection:
            try:
                self.connection.close()
                logger.info("Closed Oracle database connection")
            except Exception as e:
                logger.error(f"Error closing Oracle connection: {str(e)}")
            finally:
                self.connection = None

    def find_custom_objects(self, prefix, days_lookback=3, is_first_run=False):
        """
        Find all custom objects with the specified prefix

        Args:
            prefix (str): Object name prefix to search for
            days_lookback (int): Number of days to look back for modified objects
            is_first_run (bool): If True, scan all objects regardless of modification date

        Returns:
            list: List of dictionaries containing object metadata
        """
        if not self.connection:
            logger.error("Not connected to database")
            return []

        cursor = self.connection.cursor()

        if is_first_run:
            logger.info("First run detected - will scan all objects")
            query = """
            SELECT o.OWNER, o.OBJECT_NAME, o.OBJECT_TYPE, o.LAST_DDL_TIME
            FROM ALL_OBJECTS o
            WHERE o.OBJECT_NAME LIKE :obj_prefix || '%'
            AND o.OBJECT_TYPE IN ('FUNCTION', 'PROCEDURE', 'PACKAGE', 'PACKAGE BODY', 'TRIGGER', 'VIEW', 'TYPE', 'TYPE BODY')
            ORDER BY o.OWNER, o.OBJECT_TYPE, o.OBJECT_NAME
            """
            cursor.execute(query, {"obj_prefix": prefix})
        else:
            logger.info(f"Scanning only objects modified in the last {days_lookback} days")

            query = """
            SELECT o.OWNER, o.OBJECT_NAME, o.OBJECT_TYPE, o.LAST_DDL_TIME
            FROM ALL_OBJECTS o
            WHERE o.OBJECT_NAME LIKE :obj_prefix || '%'
            AND o.OBJECT_TYPE IN ('FUNCTION', 'PROCEDURE', 'PACKAGE', 'PACKAGE BODY', 'TRIGGER', 'VIEW', 'TYPE', 'TYPE BODY')
            AND o.LAST_DDL_TIME > SYSDATE - :days_lookback
            ORDER BY o.OWNER, o.OBJECT_TYPE, o.OBJECT_NAME
            """
            cursor.execute(query, {"obj_prefix": prefix, "days_lookback": days_lookback})

        objects = []
        for row in cursor:
            objects.append({
                "schema": row[0],
                "object_name": row[1],
                "object_type": row[2],
                "last_modified": (row[3].strftime("%Y-%m-%d %H:%M:%S") if row[3] else None),
            })

        cursor.close()

        logger.info(
            f"Found {len(objects)} custom objects with prefix '{prefix}'" +
            (f" modified in the last {days_lookback} days" if not is_first_run else "")
        )
        return objects

    def fetch_object_source(self, schema, object_name, object_type):
        """
        Fetch source code for a specific database object

        Args:
            schema (str): Schema/owner name
            object_name (str): Object name
            object_type (str): Object type

        Returns:
            str: Source code or None if not found
        """
        if not self.connection:
            logger.error("Not connected to database")
            return None

        cursor = self.connection.cursor()

        query = """
        SELECT LINE, TEXT 
        FROM ALL_SOURCE 
        WHERE OWNER = :schema
        AND NAME = :obj_name 
        AND TYPE = :obj_type
        ORDER BY LINE
        """

        cursor.execute(query, {
            "schema": schema,
            "obj_name": object_name,
            "obj_type": object_type
        })

        source_lines = cursor.fetchall()

        if not source_lines:
            cursor.close()
            return None

        source_text = "".join([line[1] for line in source_lines])
        cursor.close()

        return source_text