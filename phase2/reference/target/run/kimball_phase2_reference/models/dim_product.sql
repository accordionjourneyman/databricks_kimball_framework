
    -- back compat for old kwarg name
  
  
  
      
          
              
              
          
              
              
          
      
  

  

  merge into dbt_reference.dim_product as DBT_INTERNAL_DEST
      using dim_product__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
                  DBT_INTERNAL_SOURCE.source_system = DBT_INTERNAL_DEST.source_system
               and 
                  DBT_INTERNAL_SOURCE.stock_code = DBT_INTERNAL_DEST.stock_code
              

      when matched then update set
         * 

      when not matched then insert *
