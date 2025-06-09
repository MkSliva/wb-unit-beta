import { useEffect, useState } from "react";
import { LineChart, Line, CartesianGrid, XAxis, YAxis, Tooltip, Legend } from "recharts";

export default function Dashboard() {
  const [data, setData] = useState([]);
  const [groupDetails, setGroupDetails] = useState([]);
  const [selectedImt, setSelectedImt] = useState(null);
  const [startDate, setStartDate] = useState("2025-06-06");
  const [endDate, setEndDate] = useState("2025-06-09");
  const [sortConfig, setSortConfig] = useState({ key: null, direction: "asc" });
  const [showModal, setShowModal] = useState(false);
  const [chartData, setChartData] = useState([]);

  useEffect(() => {
    fetch(`http://localhost:8000/api/sales_grouped_detailed_range?start_date=${startDate}&end_date=${endDate}`)
      .then((res) => res.json())
      .then((json) => {
        setData(json.data);
        setGroupDetails([]);
        setChartData([]);
        setSelectedImt(null);
      });
  }, [startDate, endDate]);

  const fetchGroupDetails = async (imtID) => {
    try {
      const [detailsRes, chartRes] = await Promise.all([
        fetch(
          `http://localhost:8000/api/sales_by_imt?imt_id=${imtID}&start_date=${startDate}&end_date=${endDate}`
        ),
        fetch(
          `http://localhost:8000/api/sales_by_imt_daily?imt_id=${imtID}&start_date=${startDate}&end_date=${endDate}`
        )
      ]);

      const details = await detailsRes.json();
      const chart = await chartRes.json();

      if (details.data) {
        setGroupDetails(details.data);
      }

      if (chart.data) {
        setChartData(chart.data);
      }

      setSelectedImt(imtID);
      setShowModal(true);
    } catch (error) {
      console.error("Ошибка при получении деталей:", error);
    }
  };

  const sortedData = [...data].sort((a, b) => {
    if (!sortConfig.key) return 0;
    const aValue = a[sortConfig.key];
    const bValue = b[sortConfig.key];
    if (aValue < bValue) return sortConfig.direction === "asc" ? -1 : 1;
    if (aValue > bValue) return sortConfig.direction === "asc" ? 1 : -1;
    return 0;
  });

  const handleSort = (key) => {
    let direction = "asc";
    if (sortConfig.key === key && sortConfig.direction === "asc") {
      direction = "desc";
    }
    setSortConfig({ key, direction });
  };

  const totalOrders = groupDetails.reduce((acc, item) => acc + item.ordersCount, 0);
  const totalProfit = groupDetails.reduce((acc, item) => acc + item.total_profit, 0);
  const totalAd = groupDetails.reduce((acc, item) => acc + item.ad_spend, 0);
  const avgSalePrice = (groupDetails.reduce((acc, item) => acc + item.salePrice, 0) / groupDetails.length || 0).toFixed(2);
  const avgCost = (groupDetails.reduce((acc, item) => acc + item.cost_price, 0) / groupDetails.length || 0).toFixed(2);

  return (
    <div className="min-h-screen bg-gray-100 p-5 font-sans">
      <h2 className="text-2xl font-bold mb-4">📦 Группы карточек (по imtID)</h2>

      <div className="flex items-center space-x-4 mb-4">
        <label>С: </label>
        <input
          type="date"
          className="border rounded px-2 py-1"
          value={startDate}
          onChange={(e) => setStartDate(e.target.value)}
        />
        <label>По: </label>
        <input
          type="date"
          className="border rounded px-2 py-1"
          value={endDate}
          onChange={(e) => setEndDate(e.target.value)}
        />
      </div>

      <table className="table-auto w-full text-center bg-white shadow-md">
        <thead className="bg-gray-200 cursor-pointer">
          <tr>
            <th className="p-2" onClick={() => handleSort("imtID")}>imtID</th>
            <th className="p-2" onClick={() => handleSort("ordersCount")}>Заказы</th>
            <th className="p-2" onClick={() => handleSort("ad_spend")}>Реклама</th>
            <th className="p-2" onClick={() => handleSort("total_profit")}>Прибыль</th>
          </tr>
        </thead>
        <tbody>
          {sortedData.map((group, index) => (
            <tr
              key={index}
              onClick={() => fetchGroupDetails(group.imtID)}
              className="border-b hover:bg-gray-50 cursor-pointer"
            >
              <td className="py-2">{group.imtID}</td>
              <td>{group.ordersCount}</td>
              <td>{group.ad_spend}</td>
              <td>{group.total_profit}</td>
            </tr>
          ))}
        </tbody>
      </table>

      {showModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center">
          <div className="bg-white p-8 rounded-lg w-3/4 max-h-[90vh] overflow-y-auto">
            <h3 className="text-xl font-semibold mb-2">📊 Статистика по связке (imtID: {selectedImt})</h3>
            <p><strong>Заказов:</strong> {totalOrders}</p>
            <p><strong>Прибыль:</strong> {totalProfit.toFixed(2)} ₽</p>
            <p><strong>Реклама:</strong> {totalAd.toFixed(2)} ₽</p>
            <p><strong>Средняя цена продажи:</strong> {avgSalePrice} ₽</p>
            <p><strong>Средняя себестоимость:</strong> {avgCost} ₽</p>

            {chartData.length > 0 && (
              <div className="my-4 flex justify-center">
                <LineChart width={700} height={300} data={chartData}>
                  <CartesianGrid stroke="#ccc" />
                  <XAxis dataKey="date" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Line type="monotone" dataKey="ordersCount" stroke="#8884d8" name="Заказы" />
                  <Line type="monotone" dataKey="ad_spend" stroke="#82ca9d" name="Реклама" />
                  <Line type="monotone" dataKey="total_profit" stroke="#ff7300" name="Прибыль" />
                </LineChart>
              </div>
            )}

            <h4 className="text-lg font-semibold mt-4">📦 Все товары в связке:</h4>
            <table className="table-auto w-full text-center">
              <thead className="bg-gray-100">
                <tr>
                  <th className="p-2">vendorCode</th>
                  <th className="p-2">Заказы</th>
                  <th className="p-2">Реклама</th>
                  <th className="p-2">Прибыль</th>
                  <th className="p-2">Цена продажи</th>
                  <th className="p-2">Себестоимость</th>
                </tr>
              </thead>
              <tbody>
                {groupDetails.map((item, index) => (
                  <tr key={index} className="border-b">
                    <td className="py-1">{item.vendorCode}</td>
                    <td>{item.ordersCount}</td>
                    <td>{item.ad_spend}</td>
                    <td>{item.total_profit}</td>
                    <td>{item.salePrice}</td>
                    <td>{item.cost_price}</td>
                  </tr>
                ))}
              </tbody>
            </table>

            <button
              onClick={() => {
                setShowModal(false);
                setChartData([]);
                setGroupDetails([]);
              }}
              className="mt-4 bg-blue-500 text-white px-4 py-2 rounded hover:bg-blue-600"
            >
              Закрыть
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

