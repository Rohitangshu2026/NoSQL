package fragment;
public class Router {
    private final int numFragments;

    public Router(int numFragments) {
        this.numFragments = numFragments;
    }

    public int getFragmentId(String key) {
        return Math.abs(key.hashCode()) % numFragments;
    }
}
