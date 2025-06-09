import pandas as pd
import ib_insync as ib 
from datetime import datetime
from abc import ABC, abstractmethod

def get_all_contracts_from_IB(
        contract:ib.Future,
        date:str | datetime | pd.Timestamp,
        IBclient:ib.IB,
        n_days_before_expiration:int = 50,
    ) -> list[ib.Future]:
        
        """
        Get all the future contracts that are valid for a given date. 
        Valid means that the date is at least n_days_before_expiration days 
        before the expiration date.

        The contracts are sorted by expiration date.
        """
    
        if isinstance(date, str):
            date = pd.to_datetime(date)
        
        all_contracts:list[ib.ContractDetails] = IBclient.reqContractDetails(contract)
        
        if not all_contracts:
            raise ValueError("No contracts found")
        
        valid_contracts = []
        for c in all_contracts:
            expiry = pd.to_datetime(c.contract.lastTradeDateOrContractMonth, format='%Y%m%d')
            if expiry > date + pd.Timedelta(days=n_days_before_expiration):
                valid_contracts.append(
                    (expiry, c.contract)
                )
        
        valid_contracts.sort(key=lambda x: x[0])
        return [contract for expiry, contract in valid_contracts]

def get_front_month_contract_from_IB(
        contract:ib.Future,
        date:str | datetime | pd.Timestamp,
        IBclient:ib.IB,
        n_th_contrat:int = 1,
        n_days_before_expiration:int = 50,
    ) -> ib.Future:

    """
    Get the n_th_contrat front month contract for the given date. 
    Which expires in at least n_days_before_expiration days.
    """
    
    return get_all_contracts_from_IB(
        contract=contract,
        date=date,
        IBclient=IBclient,
        n_days_before_expiration=n_days_before_expiration
    )[n_th_contrat]


class BaseSignal(ABC):
    
    contract: ib.Future | dict[str, ib.Future]

    @abstractmethod
    def get_value(self, date:str) -> dict[str, float] | pd.Series:
        pass 

class CrudeOilTFSignal(BaseSignal):
    def __init__(self, IBclient: ib.IB, smooth: int = 1, lookback: int = 30):
        self.smooth = smooth
        self.lookback = lookback
        self.IBclient = IBclient

    def load_data(self, date: str | datetime | pd.Timestamp, lookback: int = 30):

        assert self.IBclient.isConnected(), 'Failed to connect to IB'    

        if isinstance(date, str):
            date = pd.to_datetime(date)

        self.contract = get_front_month_contract_from_IB(
            contract=ib.Future(symbol='CL', exchange='NYMEX', includeExpired=False),
            date=date,
            IBclient=self.IBclient
        )

        bars = self.IBclient.reqHistoricalData(
            self.contract,
            endDateTime=date.tz_localize('UTC'),
            durationStr=f'{lookback} D',
            barSizeSetting='1 day',
            whatToShow='TRADES',
            useRTH=True,
            formatDate=1
        )
        assert bars, 'No data'

        df = ib.util.df(bars)
        self.close = df['close'].astype(float)  
        self.close.index = pd.to_datetime(df.index)  

    def get_value(self, date: str | datetime | pd.Timestamp) -> float:

        if isinstance(date, str):
            date = pd.to_datetime(date)
    
        self.load_data(date, self.lookback)

        short_term_group = self.close.ffill().fillna(0).ewm(self.smooth)
        long_term_group = self.close.ffill().fillna(0).ewm(self.lookback)

        ma_short_term = short_term_group.mean().iloc[-1] 
        ma_long_term = long_term_group.mean().iloc[-1] 
        std_long_term = long_term_group.std().iloc[-1] 

        return ( ma_short_term - ma_long_term ) / std_long_term 
