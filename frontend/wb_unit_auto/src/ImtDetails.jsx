import React, { useEffect, useState } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";

const ImtDetails = ({ imtId, startDate, endDate, goBack }) => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [groupDetails, setGroupDetails] = useState([]);
  const [dailySales, setDailySales] = useState([]);
  const [purchasePriceHistory, setPurchasePriceHistory] = useState([]);
  const [purchaseBatches, setPurchaseBatches] = useState({});
  const [managerInfo, setManagerInfo] = useState({ ad_manager_name: "", start_date: "" });

  useEffect(() => {
    const load = async () => {
      if (!imtId) return;
      setLoading(true);
      setError(null);
      try {
        const detailsResp = await fetch(
          `http://localhost:8000/api/sales_by_imt?imt_id=${imtId}&start_date=${startDate}&end_date=${endDate}`
        );
        if (!detailsResp.ok) throw new Error("Failed to load details");
        const detailsData = await detailsResp.json();
        setGroupDetails(detailsData.data);

        const dailyResp = await fetch(
          `http://localhost:8000/api/sales_by_imt_daily?imt_id=${imtId}&start_date=${startDate}&end_date=${endDate}`
        );
        if (dailyResp.ok) {
          const d = await dailyResp.json();
          setDailySales(d.data);
        }

        for (const item of detailsData.data) {
          const batchResp = await fetch(
            `http://localhost:8000/api/purchase_batches?vendor_code=${item.vendorcode}`
          );
          if (batchResp.ok) {
            const batches = await batchResp.json();
            setPurchaseBatches((prev) => ({ ...prev, [item.vendorcode]: batches }));
          }
        }

        if (detailsData.data.length > 0) {
          const firstVendor = detailsData.data[0].vendorcode;
          const histResp = await fetch(
            `http://localhost:8000/api/purchase_price_history_daily?vendor_code=${firstVendor}&start_date=${startDate}&end_date=${endDate}`
          );
          if (histResp.ok) {
            const hist = await histResp.json();
            setPurchasePriceHistory(hist);
          }
        }

        const managerResp = await fetch(`http://localhost:8000/api/manager_info?imt_id=${imtId}`);
        if (managerResp.ok) {
          const m = await managerResp.json();
          setManagerInfo({
            ad_manager_name: m.ad_manager_name || "",
            start_date: m.start_date || "",
          });
        }
      } catch (e) {
        console.error(e);
        setError("Не удалось загрузить детали");
      } finally {
        setLoading(false);
      }
    };
    load();
  }, [imtId, startDate, endDate]);

  return (
    <div className="p-4">
      <div className="flex justify-between items-center mb-4">
        <h1 className="text-xl font-semibold">Детали по IMT ID: {imtId}</h1>
        <button onClick={goBack} className="px-4 py-2 bg-gray-300 rounded hover:bg-gray-400">
          Назад
        </button>
      </div>
      {loading ? (
        <p>Загрузка...</p>
      ) : error ? (
        <p className="text-red-600">{error}</p>
      ) : (
        <>
          {managerInfo.ad_manager_name && managerInfo.ad_manager_name !== "0" && (
            <div className="mb-4 p-2 border rounded">
              На ведении у менеджера {managerInfo.ad_manager_name} c {managerInfo.start_date}
            </div>
          )}
          <h4 className="font-semibold mb-2">Детали по артикулам:</h4>
          <div className="overflow-x-auto mb-6">
            <table className="min-w-full divide-y divide-gray-200 border text-sm">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-4 py-2 text-left">Артикул</th>
                  <th className="px-4 py-2 text-left">Заказы</th>
                  <th className="px-4 py-2 text-left">Реклама</th>
                  <th className="px-4 py-2 text-left">Прибыль</th>
                  <th className="px-4 py-2 text-left">Ср. Цена продажи</th>
                  <th className="px-4 py-2 text-left">Ср. Себестоимость</th>
                  <th className="px-4 py-2 text-left">Ср. Закупка</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {groupDetails.map((detail) => (
                  <tr key={detail.vendorcode}>
                    <td className="px-4 py-2">{detail.vendorcode}</td>
                    <td className="px-4 py-2">{detail.orderscount}</td>
                    <td className="px-4 py-2">{detail.ad_spend.toFixed(2)}</td>
                    <td className={`px-4 py-2 ${detail.total_profit >= 0 ? "text-green-600" : "text-red-600"}`}>
                      {detail.total_profit.toFixed(2)}
                    </td>
                    <td className="px-4 py-2">{detail.actual_discounted_price.toFixed(2)}</td>
                    <td className="px-4 py-2">{detail.cost_price.toFixed(2)}</td>
                    <td className="px-4 py-2">{detail.purchase_price.toFixed(2)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <h4 className="font-semibold mb-2">История закупочных партий</h4>
          {Object.keys(purchaseBatches).map((vc) => (
            <div key={vc} className="mb-4">
              <h5 className="font-medium mb-1">Артикул {vc}</h5>
              <table className="min-w-full divide-y divide-gray-200 border text-xs">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-2 py-1 text-left">Начало</th>
                    <th className="px-2 py-1 text-left">Конец</th>
                    <th className="px-2 py-1 text-left">Цена</th>
                    <th className="px-2 py-1 text-left">Кол-во</th>
                    <th className="px-2 py-1 text-left">Продано</th>
                    <th className="px-2 py-1 text-left">Статус</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {purchaseBatches[vc]?.map((b) => (
                    <tr key={b.start_date} className={b.is_active ? "bg-green-50" : ""}>
                      <td className="px-2 py-1">{b.start_date}</td>
                      <td className="px-2 py-1">{b.end_date || "-"}</td>
                      <td className="px-2 py-1">{b.purchase_price}</td>
                      <td className="px-2 py-1">{b.quantity_bought}</td>
                      <td className="px-2 py-1">{b.quantity_sold}</td>
                      <td className="px-2 py-1">{b.is_active ? "активна" : "закрыта"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ))}

          <h4 className="font-semibold mb-2">История закупочной цены ({groupDetails[0]?.vendorcode || "N/A"})</h4>
          {purchasePriceHistory.length > 0 ? (
            <div style={{ width: "100%", height: 300 }}>
              <ResponsiveContainer>
                <LineChart data={purchasePriceHistory} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Line type="monotone" dataKey="purchase_price" stroke="#8884d8" name="Закупочная цена" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <p className="text-gray-600">Нет данных для графика закупочной цены.</p>
          )}

          <h4 className="font-semibold mb-2 mt-6">Ежедневная динамика (Заказы, Прибыль, Реклама)</h4>
          {dailySales.length > 0 ? (
            <div style={{ width: "100%", height: 300 }}>
              <ResponsiveContainer>
                <LineChart data={dailySales} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" />
                  <YAxis yAxisId="left" orientation="left" />
                  <YAxis yAxisId="right" orientation="right" />
                  <Tooltip />
                  <Legend />
                  <Line yAxisId="left" type="monotone" dataKey="orderscount" stroke="#82ca9d" name="Заказы" />
                  <Line yAxisId="left" type="monotone" dataKey="total_profit" stroke="#8884d8" name="Прибыль" />
                  <Line yAxisId="right" type="monotone" dataKey="ad_spend" stroke="#ffc658" name="Реклама" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <p className="text-gray-600">Данные по ежедневной динамике недоступны для выбранного периода.</p>
          )}
        </>
      )}
    </div>
  );
};

export default ImtDetails;

