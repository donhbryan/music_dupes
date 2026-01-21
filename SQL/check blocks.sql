SELECT DISTINCT path  from files 
where path like  "%Beyt Bieh%"

SELECT DISTINCT path FROM fingerprint_index 
--where path like  "%Beyt Bieh%"
WHERE block IN ('AQADtBKlJpFSHGdG', 'XMGTH32XpPC-oWXw', 'S0R08E0Ou8T4oUme', 'of3xHJN9COZxHT9E', 'vEvxD37R82jaBP2O', 'H9qRAy1RHhdOvBn4', 'YxryMBeSdgl-41ka', 'PBFRmTMaHZeiE3dw')
