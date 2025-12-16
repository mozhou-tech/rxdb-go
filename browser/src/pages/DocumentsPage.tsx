import { useState, useEffect } from 'react'
import { apiClient, Document } from '../utils/api'
import { Button } from '../components/ui/Button'
import { Input } from '../components/ui/Input'
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/Card'

export default function DocumentsPage() {
  const [collection, setCollection] = useState('articles')
  const [documents, setDocuments] = useState<Document[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [skip, setSkip] = useState(0)
  const [limit] = useState(20)
  const [total, setTotal] = useState(0)

  const loadDocuments = async () => {
    if (!collection) return

    setLoading(true)
    setError(null)
    try {
      const response = await apiClient.getDocuments(collection, skip, limit)
      setDocuments(response.documents)
      setTotal(response.total)
    } catch (err: any) {
      setError(err.message || '加载文档失败')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadDocuments()
  }, [collection, skip])

  const handleDelete = async (id: string) => {
    if (!confirm('确定要删除这个文档吗？')) return

    try {
      await apiClient.deleteDocument(collection, id)
      loadDocuments()
    } catch (err: any) {
      alert('删除失败: ' + err.message)
    }
  }

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>文档浏览</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex gap-4 mb-4">
            <Input
              placeholder="集合名称 (例如: articles)"
              value={collection}
              onChange={(e) => {
                setCollection(e.target.value)
                setSkip(0)
              }}
            />
            <Button onClick={loadDocuments} disabled={loading}>
              {loading ? '加载中...' : '刷新'}
            </Button>
          </div>

          {error && (
            <div className="mb-4 p-4 bg-destructive/10 text-destructive rounded-md">
              {error}
            </div>
          )}

          <div className="mb-4 text-sm text-muted-foreground">
            共 {total} 个文档，显示 {skip + 1}-{Math.min(skip + limit, total)} 个
          </div>

          <div className="space-y-4">
            {documents.map((doc) => (
              <Card key={doc.id}>
                <CardContent className="pt-6">
                  <div className="flex justify-between items-start">
                    <div className="flex-1">
                      <div className="font-semibold mb-2">ID: {doc.id}</div>
                      <pre className="text-sm bg-muted p-4 rounded-md overflow-auto">
                        {JSON.stringify(doc.data, null, 2)}
                      </pre>
                    </div>
                    <Button
                      variant="destructive"
                      size="sm"
                      onClick={() => handleDelete(doc.id)}
                      className="ml-4"
                    >
                      删除
                    </Button>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>

          {documents.length === 0 && !loading && (
            <div className="text-center py-8 text-muted-foreground">
              没有找到文档
            </div>
          )}

          <div className="flex gap-2 mt-4">
            <Button
              variant="outline"
              onClick={() => setSkip(Math.max(0, skip - limit))}
              disabled={skip === 0 || loading}
            >
              上一页
            </Button>
            <Button
              variant="outline"
              onClick={() => setSkip(skip + limit)}
              disabled={skip + limit >= total || loading}
            >
              下一页
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

