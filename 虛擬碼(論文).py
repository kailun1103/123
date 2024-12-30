Procedure DustDetection(mempoolTxn)
    for txn in mempoolTxn:
        txn_info = MonitorMempool(txn) // 從內存池獲得交易相關資訊
        txn['TxnInitiationDate'] = txn_info.get('txnTime')  // 獲取交易創建時間
        txn['TxnInputAmount'] = txn_info.get('txnInputAmount')  // 獲取交易輸入金額
        txn['TxnOutputAmount'] = txn_info.get('txnOutputAmount')  // 獲取交易輸出金額
        txn['TxnFee'] = txn_info.get('txnFee')  // 獲取交易手續費
        txn['MempoolTxnCount'] = txn_info.get('mempool')  // 監聽內存池即時壅塞程度
        txn['feeRatio'] = txn[fee] / txn[txnOutputAmount]  //計算交易手續費在金額占比
        if txn['feeRatio'] >= 20%  //從手續費占比判斷此交易為粉塵
            txn['dustBool'] = 1  //占比超過20%將此交易視為粉塵
        else
            txn['dustBool'] =  0 //占比小於20%將此交易視為正常
        end if
    return mempoolTxn
end procedure