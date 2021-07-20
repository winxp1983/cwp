
class MyDbUtil():
    def __init__(self,conn,tabname,id,fileds=None):
        self._conn=conn
        self._tabname=tabname
        self._id=id
        self._data={}
        if fileds == None:
            self._fields=self.getFields()
        else:
            self._fields = fileds
        self.isNewRecord=False
        self.idStr=False

    def __setitem__(self, key, value):
        if key.lower() in self._fields:
            self._data[key.lower()]=value.strip() if isinstance(value,str) else value

    def __getitem__(self, item):
        return self._data.get(item.lower())

    def getFields(self):
        cursor = self._conn.cursor()
        cursor.execute(f"select * from {self._tabname}")
        fields = [tuple[0] for tuple in cursor.description ]
        cursor.close()
        return fields

    def close(self):
        self._conn.close()

    def new(self):
        self._data={}


    def insert(self):
        keys = ','.join(self._data.keys())
        values = ','.join( [f"'{val}'" for val in self._data.values()])
        sql=f"insert into {self._tabname} ({keys}) values ({values})"
        cursor = self._conn.cursor()
        try:
            row = cursor.execute(sql)
            self._conn.commit()
            return row
        except Exception as e:
            self._conn.rollback()
            print(e)
        finally:
            cursor.close()


    def replace(self):
        values = ','.join( [f"{k}='{v}'" for k,v in self._data.items()])
        sql = f"replace into {self._tabname} set {values}"
        cursor = self._conn.cursor()
        try:
            row = cursor.execute(sql)
            self._conn.commit()
            return row
        except Exception as e:
            self._conn.rollback()
            print(e)
        finally:
            cursor.close()


    def find(self,sql):
        pass

    def findAll(self,sql):
        sql=f"select * from {self._tabname}  {sql}"
        cursor = self._conn.cursor()
        cursor.execute(sql)
        reslut = cursor.fetchall()
        cursor.close()
        return reslut

    def deleteAll(self):
        pass
    def deleteById(self,idValues):
        value=["{k}='{v}'".format(k=k,v=str(v).strip()) for k,v in idValues.items()]
        values = ' and '.join(value)
        sql=f"delete from {self._tabname} where {values}"

    def findById(self,idValues):
        value=["{k}='{v}'".format(k=k,v=str(v).strip()) for k,v in idValues.items()]
        values = ' and '.join(value)
        sql=f"select * from {self._tabname} where {values}"
        cursor = self._conn.cursor()
        cursor.execute(sql)
        reslut = cursor.fetchone()
        cursor.close()
        if reslut:
            self._data=reslut
            return True
        else:
            return False

    def idValues(self):
        values=[]
        for k in self._fields:
            if k in self._data:
                values.append("'{val}'".format(val=self._data[k]))
            else:
                values.append("''")
        return values

    def reValues(self):
        values=[]
        for k in self._fields:
            if k in self._data:
                values.append("{k}='{v}'".format(k=k,v=self._data[k]))
        return values



