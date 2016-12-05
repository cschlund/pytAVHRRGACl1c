
from config_add_sql_metadata import *
from quick_l1c_analysis import QuickDatabase
from pycmsaf.logger import setup_root_logger

logger = setup_root_logger(name='root')


class AddMetadata(QuickDatabase):
    def __init__(self, dbfile, exclusive=False, **kwargs):
        # Call __init__ of parent class
        super(QuickDatabase, self).__init__(dbfile=dbfile, **kwargs)
        # Lock database
        if exclusive:
            self.begin_exclusive()

    def add_column(self, table, new_column, new_type):
        """
        Add new column and its type into already existing table.
        :param table: table name
        :param new_column: column name
        :param new_type: type of column
        :return:
        """
        try:
            self.execute('ALTER TABLE {tab} ADD COLUMN {col} {typ}'.
                         format(tab=table, col=new_column, typ=new_type))
        except:
            pass

    def insert_metadata(self, src_tab, src_col, tar_col, tar_rec):
        """
        Insert metadata regarding the source table and column
        :param src_tab: source table name
        :param src_col: source column name
        :param tar_col: target column name
        :param tar_rec: target record
        :return:
        """
        try:
            print src_tab, src_col, tar_col, tar_rec
            query = "UPDATE {src_tab} SET {tar_col}=\'{tar_rec}\' " \
                    "WHERE name = \'{src_col}\'".format(src_tab=src_tab, src_col=src_col,
                                                        tar_col=tar_col, tar_rec=tar_rec)
            self.execute(query)
        except:
            pass


if __name__ == '__main__':

    # connect to database
    db = AddMetadata(dbfile=sql_file, timeout=3600, create=True)

    # add new column and its record
    db.add_column(table=source_table, new_column=target_column, new_type=target_type)
    for idx, rec in enumerate(target_record):
        db.insert_metadata(src_tab=source_table, src_col=source_column[idx],
                           tar_col=target_column, tar_rec=target_record[idx])
    db.commit_changes()

    # print schema
    db.print_schema()

    # print updated table
    db.printout(table=source_table)

    # close database
    db.close()
