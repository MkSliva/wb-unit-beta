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
  const [editData, setEditData] = useState({
    vendorCode: "",
    startDate: "",
    purchase_price: "",
    delivery_to_warehouse: "",
    wb_commission_rub: "",
    wb_logistics: "",
    tax_rub: "",
    packaging: "",
    fuel: "",
    gift: "",
    defect_percent: "",
  });
  const [visibleColumns, setVisibleColumns] = useState({
    ordersCount: true,
    ad_spend: true,
    total_profit: true,
  });

  const toggleColumn = (col) => {
    setVisibleColumns((prev) => ({ ...prev, [col]: !prev[col] }));
  };

  const formatMoney = (val) =>
    new Intl.NumberFormat("ru-RU", {
      style: "currency",
      currency: "RUB",
      maximumFractionDigits: 2,
    }).format(val);

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
  const avgSalePrice = groupDetails.length
    ? groupDetails.reduce((acc, item) => acc + item.salePrice, 0) / groupDetails.length
    : 0;
  const avgCost = groupDetails.length
    ? groupDetails.reduce((acc, item) => acc + item.cost_price, 0) / groupDetails.length
    : 0;

  const handleEditChange = (e) => {
    const { name, value } = e.target;
    setEditData((prev) => ({ ...prev, [name]: value }));
  };

  const submitCostUpdate = async () => {
    if (!editData.vendorCode || !editData.startDate) return;
    await fetch("http://localhost:8000/api/update_costs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(editData),
    });
    fetchGroupDetails(selectedImt);
  };

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

      <div className="flex space-x-4 mb-4">
        {[
          { key: "ordersCount", label: "Заказы" },
          { key: "ad_spend", label: "Реклама" },
          { key: "total_profit", label: "Прибыль" },
        ].map((col) => (
          <label key={col.key} className="flex items-center space-x-1">
            <input
              type="checkbox"
              checked={visibleColumns[col.key]}
              onChange={() => toggleColumn(col.key)}
            />
            <span>{col.label}</span>
          </label>
        ))}
      </div>

      <table className="table-auto w-full text-center bg-white shadow-md">
        <thead className="bg-gray-200 cursor-pointer">
          <tr>
            <th className="p-2" onClick={() => handleSort("imtID")}>imtID</th>
            {visibleColumns.ordersCount && (
              <th className="p-2" onClick={() => handleSort("ordersCount")}>Заказы</th>
            )}
            {visibleColumns.ad_spend && (
              <th className="p-2" onClick={() => handleSort("ad_spend")}>Реклама</th>
            )}
            {visibleColumns.total_profit && (
              <th className="p-2" onClick={() => handleSort("total_profit")}>Прибыль</th>
            )}
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
              {visibleColumns.ordersCount && <td>{group.ordersCount}</td>}
              {visibleColumns.ad_spend && <td>{formatMoney(group.ad_spend)}</td>}
              {visibleColumns.total_profit && <td>{formatMoney(group.total_profit)}</td>}
            </tr>
          ))}
        </tbody>
      </table>

      {showModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center">
          <div className="bg-white p-8 rounded-lg w-3/4 max-h-[90vh] overflow-y-auto">
            <h3 className="text-xl font-semibold mb-2">📊 Статистика по связке (imtID: {selectedImt})</h3>
            <p><strong>Заказов:</strong> {totalOrders}</p>
            <p><strong>Прибыль:</strong> {formatMoney(totalProfit)}</p>
            <p><strong>Реклама:</strong> {formatMoney(totalAd)}</p>
            <p><strong>Средняя цена продажи:</strong> {formatMoney(avgSalePrice)}</p>
            <p><strong>Средняя себестоимость:</strong> {formatMoney(avgCost)}</p>

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
                    <th className="p-2">Закуп</th>
                    <th className="p-2">Доставка</th>
                    <th className="p-2">Комиссия</th>
                    <th className="p-2">Логистика</th>
                    <th className="p-2">Налог</th>
                    <th className="p-2">Упак.</th>
                    <th className="p-2">Топливо</th>
                    <th className="p-2">Подарок</th>
                    <th className="p-2">Брак</th>
                  </tr>
                </thead>
                <tbody>
                  {groupDetails.map((item, index) => (
                    <tr key={index} className="border-b">
                      <td className="py-1">{item.vendorCode}</td>
                      <td>{item.ordersCount}</td>
                      <td>{formatMoney(item.ad_spend)}</td>
                      <td>{formatMoney(item.total_profit)}</td>
                      <td>{formatMoney(item.salePrice)}</td>
                      <td>{formatMoney(item.cost_price)}</td>
                      <td>{formatMoney(item.purchase_price)}</td>
                      <td>{formatMoney(item.delivery_to_warehouse)}</td>
                      <td>{formatMoney(item.wb_commission_rub)}</td>
                      <td>{formatMoney(item.wb_logistics)}</td>
                      <td>{formatMoney(item.tax_rub)}</td>
                      <td>{formatMoney(item.packaging)}</td>
                      <td>{formatMoney(item.fuel)}</td>
                      <td>{formatMoney(item.gift)}</td>
                      <td>{formatMoney(item.defect_percent)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>

              <div className="mt-4 space-y-2">
                <h4 className="font-semibold">Изменить себестоимость</h4>
                <select
                  className="border p-1"
                  name="vendorCode"
                  value={editData.vendorCode}
                  onChange={handleEditChange}
                >
                  <option value="">Товар</option>
                  {groupDetails.map((g) => (
                    <option key={g.vendorCode} value={g.vendorCode}>
                      {g.vendorCode}
                    </option>
                  ))}
                </select>
                <input
                  type="date"
                  name="startDate"
                  className="border p-1 ml-2"
                  value={editData.startDate}
                  onChange={handleEditChange}
                />
                {[
                  "purchase_price",
                  "delivery_to_warehouse",
                  "wb_commission_rub",
                  "wb_logistics",
                  "tax_rub",
                  "packaging",
                  "fuel",
                  "gift",
                  "defect_percent",
                ].map((field) => (
                  <input
                    key={field}
                    name={field}
                    type="number"
                    placeholder={field}
                    className="border p-1 ml-2 w-24"
                    value={editData[field]}
                    onChange={handleEditChange}
                  />
                ))}
                <button
                  onClick={submitCostUpdate}
                  className="ml-2 bg-green-500 text-white px-2 py-1 rounded"
                >
                  Сохранить
                </button>
              </div>

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

