path =
insertstmnt= f"""
             bulk insert abc 
             from  {path}
                with(
                format = 'CSV',
                lastrow=2,
                firstrow=2
                ) ;
             """   