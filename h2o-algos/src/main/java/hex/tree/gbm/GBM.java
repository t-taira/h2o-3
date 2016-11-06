package hex.tree.gbm;

import hex.genmodel.utils.DistributionFamily;
import hex.Distribution;
import hex.ModelCategory;
import hex.quantile.Quantile;
import hex.quantile.QuantileModel;
import hex.tree.*;
import hex.tree.DTree.DecidedNode;
import hex.tree.DTree.LeafNode;
import hex.tree.DTree.UndecidedNode;
import water.*;
import water.exceptions.H2OModelBuilderIllegalArgumentException;
import water.fvec.*;
import water.util.*;

import java.util.Arrays;
import java.util.Random;

/** Gradient Boosted Trees
 *
 *  Based on "Elements of Statistical Learning, Second Edition, page 387"
 */
public class GBM extends SharedTree<GBMModel,GBMModel.GBMParameters,GBMModel.GBMOutput> {
  @Override public ModelCategory[] can_build() {
    return new ModelCategory[]{
      ModelCategory.Regression,
      ModelCategory.Binomial,
      ModelCategory.Multinomial,
    };
  }

  // Called from an http request
  public GBM( GBMModel.GBMParameters parms                   ) { super(parms     ); init(false); }
  public GBM( GBMModel.GBMParameters parms, Key<GBMModel> key) { super(parms, key); init(false); }
  public GBM(boolean startup_once) { super(new GBMModel.GBMParameters(),startup_once); }

  @Override protected int nModelsInParallel() {
    if (!_parms._parallelize_cross_validation || _parms._max_runtime_secs != 0) return 1; //user demands serial building (or we need to honor the time constraints for all CV models equally)
    if (_train.byteSize() < 1e6) return _parms._nfolds; //for small data, parallelize over CV models
    return 2; //GBM always has some serial work, so it's fine to build two models at once
  }

  /** Start the GBM training Job on an F/J thread. */
  @Override protected GBMDriver trainModelImpl() {
    return new GBMDriver();
  }

  /** Initialize the ModelBuilder, validating all arguments and preparing the
   *  training frame.  This call is expected to be overridden in the subclasses
   *  and each subclass will start with "super.init();".  This call is made
   *  by the front-end whenever the GUI is clicked, and needs to be fast;
   *  heavy-weight prep needs to wait for the trainModel() call.
   *
   *  Validate the learning rate and distribution family. */
  @Override public void init(boolean expensive) {
    super.init(expensive);

    // Initialize response based on given distribution family.
    // Regression: initially predict the response mean
    // Binomial: just class 0 (class 1 in the exact inverse prediction)
    // Multinomial: Class distribution which is not a single value.

    // However there is this weird tension on the initial value for
    // classification: If you guess 0's (no class is favored over another),
    // then with your first GBM tree you'll typically move towards the correct
    // answer a little bit (assuming you have decent predictors) - and
    // immediately the Confusion Matrix shows good results which gradually
    // improve... BUT the Means Squared Error will suck for unbalanced sets,
    // even as the CM is good.  That's because we want the predictions for the
    // common class to be large and positive, and the rare class to be negative
    // and instead they start around 0.  Guessing initial zero's means the MSE
    // is so bad, that the R^2 metric is typically negative (usually it's
    // between 0 and 1).

    // If instead you guess the mean (reversed through the loss function), then
    // the zero-tree GBM model reports an MSE equal to the response variance -
    // and an initial R^2 of zero.  More trees gradually improves the R^2 as
    // expected.  However, all the minority classes have large guesses in the
    // wrong direction, and it takes a long time (lotsa trees) to correct that
    // - so your CM sucks for a long time.
    if (expensive) {
      if (error_count() > 0)
        throw H2OModelBuilderIllegalArgumentException.makeFromBuilder(GBM.this);
      if (_parms._distribution == DistributionFamily.AUTO) {
        if (_nclass == 1) _parms._distribution = DistributionFamily.gaussian;
        if (_nclass == 2) _parms._distribution = DistributionFamily.bernoulli;
        if (_nclass >= 3) _parms._distribution = DistributionFamily.multinomial;
      }
      checkDistributions();
      if (hasOffsetCol() && isClassifier() && _parms._distribution == DistributionFamily.multinomial) {
        error("_offset_column", "Offset is not supported for multinomial distribution.");
      }
      if (hasOffsetCol() && _parms._distribution == DistributionFamily.bernoulli) {
        if (_offset.max() > 1)
          error("_offset_column", "Offset cannot be larger than 1 for Bernoulli distribution.");
      }
      if (hasOffsetCol() && _parms._distribution == DistributionFamily.modified_huber) {
        if (_offset.max() > 1)
          error("_offset_column", "Offset cannot be larger than 1 for Modified Huber distribution.");
      }
    }

    switch( _parms._distribution) {
    case bernoulli:
      if( _nclass != 2 /*&& !couldBeBool(_response)*/)
        error("_distribution", H2O.technote(2, "Binomial requires the response to be a 2-class categorical"));
      break;
    case modified_huber:
      if( _nclass != 2 /*&& !couldBeBool(_response)*/)
        error("_distribution", H2O.technote(2, "Modified Huber requires the response to be a 2-class categorical."));
      break;
    case multinomial:
      if (!isClassifier()) error("_distribution", H2O.technote(2, "Multinomial requires an categorical response."));
      break;
    case huber:
      if (isClassifier()) error("_distribution", H2O.technote(2, "Huber requires the response to be numeric."));
      break;
    case poisson:
      if (isClassifier()) error("_distribution", H2O.technote(2, "Poisson requires the response to be numeric."));
      break;
    case gamma:
      if (isClassifier()) error("_distribution", H2O.technote(2, "Gamma requires the response to be numeric."));
      break;
    case tweedie:
      if (isClassifier()) error("_distribution", H2O.technote(2, "Tweedie requires the response to be numeric."));
      break;
    case gaussian:
      if (isClassifier()) error("_distribution", H2O.technote(2, "Gaussian requires the response to be numeric."));
      break;
    case laplace:
      if (isClassifier()) error("_distribution", H2O.technote(2, "Laplace requires the response to be numeric."));
      break;
    case quantile:
      if (isClassifier()) error("_distribution", H2O.technote(2, "Quantile requires the response to be numeric."));
      break;
    case AUTO:
      break;
    default:
      error("_distribution","Invalid distribution: " + _parms._distribution);
    }

    if( !(0. < _parms._learn_rate && _parms._learn_rate <= 1.0) )
      error("_learn_rate", "learn_rate must be between 0 and 1");
    if( !(0. < _parms._learn_rate_annealing && _parms._learn_rate_annealing <= 1.0) )
      error("_learn_rate_annealing", "learn_rate_annealing must be between 0 and 1");
    if( !(0. < _parms._col_sample_rate && _parms._col_sample_rate <= 1.0) )
      error("_col_sample_rate", "col_sample_rate must be between 0 and 1");
    if (_parms._max_abs_leafnode_pred <= 0)
      error("_max_abs_leafnode_pred", "max_abs_leafnode_pred must be larger than 0.");
    if (_parms._pred_noise_bandwidth < 0)
      error("_pred_noise_bandwidth", "pred_noise_bandwidth must be >= 0.");
  }

  // ----------------------
  private class GBMDriver extends Driver {
    @Override protected boolean doOOBScoring() { return false; }
    @Override protected void initializeModelSpecifics() {
      if(_parms._use_new_histo_tsk){
        _scbParms = new ScoreBuildHistogram2.SCBParms();
        _scbParms.blockSz = _parms._col_block_sz;
        _scbParms.sharedHisto = _parms._shared_histo;
        _scbParms.min_threads = _parms._min_threads == -1?H2O.NUMCPUS:_parms._min_threads;
        _scbParms._unordered = _parms._unordered;
      }
      _mtry_per_tree = Math.max(1, (int)(_parms._col_sample_rate_per_tree * _ncols)); //per-tree
      if (!(1 <= _mtry_per_tree && _mtry_per_tree <= _ncols)) throw new IllegalArgumentException("Computed mtry_per_tree should be in interval <1,"+_ncols+"> but it is " + _mtry_per_tree);
      _mtry = Math.max(1, (int)(_parms._col_sample_rate * _parms._col_sample_rate_per_tree * _ncols)); //per-split
      if (!(1 <= _mtry && _mtry <= _ncols)) throw new IllegalArgumentException("Computed mtry should be in interval <1,"+_ncols+"> but it is " + _mtry);

      // for Bernoulli, we compute the initial value with Newton-Raphson iteration, otherwise it might be NaN here
      DistributionFamily distr = _parms._distribution;
      _initialPrediction = _nclass > 2 || distr == DistributionFamily.laplace || distr == DistributionFamily.huber || distr == DistributionFamily.quantile ? 0 : getInitialValue();
      if (distr == DistributionFamily.bernoulli) {
        if (hasOffsetCol())
          _initialPrediction = getInitialValueBernoulliOffset(_train);
      } else if (distr == DistributionFamily.laplace || distr == DistributionFamily.huber) {
        _initialPrediction = getInitialValueQuantile(0.5);
      } else if (distr == DistributionFamily.quantile) {
        _initialPrediction = getInitialValueQuantile(_parms._quantile_alpha);
      }
      _model._output._init_f = _initialPrediction; //always write the initial value here (not just for Bernoulli)

      // Set the initial prediction into the tree column 0
      if( _initialPrediction != 0.0 ) {
        final double init = _initialPrediction;
        new MRTask() {
          @Override
          public void map(Chunk tree) {
            for (int i = 0; i < tree._len; i++) tree.set(i, init);
          }
        }.doAll(vec_tree(_train, 0), _parms._build_tree_one_node); // Only setting tree-column 0
      }
    }

    /**
     * Helper to compute the initial value for Laplace/Huber/Quantile (incl. optional offset and obs weights)
     * @return weighted median of response - offset
     */
    private double getInitialValueQuantile(double quantile) {
      // obtain y - o
      Vec y = hasOffsetCol() ? new MRTask() {
        @Override public void map(Chunk[] chks, NewChunk[] nc) {
          final Chunk resp = chk_resp(chks);
          final Chunk offset = chk_offset(chks);
          for (int i=0; i<chks[0]._len; ++i)
            nc[0].addNum(resp.atd(i) - offset.atd(i)); //y - o
        }
      }.doAll(1, (byte)3 /*numeric*/, _train).outputFrame().anyVec() : response();

      // Now compute (weighted) quantile of y - o
      double res = Double.NaN;
      QuantileModel qm = null;
      Frame tempFrame = null;
      try {
        tempFrame = new Frame(Key.<Frame>make(H2O.SELF), new String[]{"y"}, new Vec[]{y});
        if (hasWeightCol()) tempFrame.add("w", _weights);
        DKV.put(tempFrame);
        QuantileModel.QuantileParameters parms = new QuantileModel.QuantileParameters();
        parms._train = tempFrame._key;
        parms._probs = new double[]{quantile};
        parms._weights_column = hasWeightCol() ? "w" : null;
        Job<QuantileModel> job1 = new Quantile(parms).trainModel();
        qm = job1.get();
        res = qm._output._quantiles[0][0];
      } finally {
        if (qm!=null) qm.remove();
        if (tempFrame!=null) DKV.remove(tempFrame._key);
      }
      return res;
    }

    /**
     * Helper to compute the initial value for Bernoulli for offset != 0
     */
    private double getInitialValueBernoulliOffset(Frame train) {
      Log.info("Running Newton-Raphson iteration to find the initial value since offsets are specified.");
      double delta;
      int count = 0;
      double tol = 1e-4;

      //From R GBM vignette:
      //For speed, gbm() does only one step of the Newton-Raphson algorithm
      //rather than iterating to convergence. No appreciable loss of accuracy
      //since the next boosting iteration will simply correct for the prior iterations
      //inadequacy.
      int N = 1; //one step is enough - same as R

      double init = 0; //start with initial value of 0 for convergence
      do {
        double newInit = new NewtonRaphson(init).doAll(train).value();
        delta = Math.abs(init - newInit);
        init = newInit;
        Log.info("Iteration " + ++count + ": initial value: " + init);
      } while (count < N && delta >= tol);
      if (delta > tol) Log.warn("Not fully converged.");
      Log.info("Newton-Raphson iteration ran for " + count + " iteration(s). Final residual: " + delta);
      return init;
    }

    /**
     * Newton-Raphson fixpoint iteration to find a self-consistent initial value
     */
    private class NewtonRaphson extends MRTask<NewtonRaphson> {
      double _init;
      double _num;
      double _denom;
      public double value() {
        return _init + _num/_denom;
      }
      NewtonRaphson(double init) { _init = init; }
      @Override public void map( Chunk chks[] ) {
        Chunk ys = chk_resp(chks);
        Chunk offset = chk_offset(chks);
        Chunk weight = hasWeightCol() ? chk_weight(chks) : new C0DChunk(1, chks[0]._len);
        Distribution dist = new Distribution(_parms);
        for( int row = 0; row < ys._len; row++) {
          double w = weight.atd(row);
          if (w == 0) continue;
          if (ys.isNA(row)) continue;
          double y = ys.atd(row);
          double o = offset.atd(row);
          double p = dist.linkInv(o + _init);
          _num += w*(y-p);
          _denom += w*p*(1.-p);
        }
      }

      @Override
      public void reduce(NewtonRaphson mrt) {
        _num += mrt._num;
        _denom += mrt._denom;
      }
    }

    // --------------------------------------------------------------------------
    // Compute Residuals
    // Do this for all rows, whether OOB or not
    class ComputePredAndRes extends MRTask<ComputePredAndRes> {
      @Override public void map( Chunk chks[] ) {
        Chunk ys = chk_resp(chks);
        Chunk offset = hasOffsetCol() ? chk_offset(chks) : new C0DChunk(0, chks[0]._len);
        Chunk preds = chk_tree(chks, 0); // Prior tree sums
        Chunk wk = chk_work(chks, 0); // Place to store residuals
        Chunk weights = hasWeightCol() ? chk_weight(chks) : new C0DChunk(1, chks[0]._len);
        double fs[] = _nclass > 1 ? new double[_nclass+1] : null;
        Distribution dist = new Distribution(_parms);
        for( int row = 0; row < wk._len; row++) {
          double weight = weights.atd(row);
          if (weight == 0) continue;
          if (ys.isNA(row)) continue;
          double f = preds.atd(row) + offset.atd(row);
          double y = ys.atd(row);
//          Log.info(f + " vs " + y); //expect that the model predicts very negative values for 0 and very positive values for 1
          if( _parms._distribution == DistributionFamily.multinomial ) {
            double sum = score1(chks, weight,0.0 /*offset not used for multiclass*/,fs,row);
            if( Double.isInfinite(sum) ) { // Overflow (happens for constant responses)
              for (int k = 0; k < _nclass; k++) {
                wk = chk_work(chks, k);
                wk.set(row, ((int)y == k ? 1f : 0f) - (Double.isInfinite(fs[k + 1]) ? 1.0f : 0.0f));
              }
            } else {
              for( int k=0; k<_nclass; k++ ) { // Save as a probability distribution
                if( _model._output._distribution[k] != 0 ) {
                  wk = chk_work(chks, k);
                  wk.set(row, ((int)y == k ? 1f : 0f) - (float)(fs[k + 1] / sum));
                }
              }
            }
          } else {
            wk.set(row, (float) dist.negHalfGradient(y, f));
          }
        }
      }
    }

    class ComputeMinMax extends MRTask<ComputeMinMax> {
      public ComputeMinMax(int firstLeafIndex, int totalNumNodes) {
        _firstLeafIndex = firstLeafIndex;
        _totalNumNodes = totalNumNodes;
      }
      int _firstLeafIndex;
      int _totalNumNodes;
      float[] _mins;
      float[] _maxs;
      @Override public void map( Chunk chks[] ) {
        int _len = _totalNumNodes - _firstLeafIndex; //number of leaves
        _mins = new float[_len];
        _maxs = new float[_len];
        Arrays.fill(_mins, Float.MAX_VALUE);
        Arrays.fill(_maxs, -Float.MAX_VALUE);

        Chunk ys = chk_resp(chks);
        Chunk offset = hasOffsetCol() ? chk_offset(chks) : new C0DChunk(0, chks[0]._len);
        Chunk preds = chk_tree(chks, 0); // Prior tree sums
        Chunk nids = chk_nids(chks, 0);
        Chunk weights = hasWeightCol() ? chk_weight(chks) : new C0DChunk(1, chks[0]._len);
        for( int row = 0; row < preds._len; row++) {
          if( ys.isNA(row) ) continue;
          if (weights.atd(row)==0) continue;
          int nid = (int)nids.at8(row);
          assert(nid!=ScoreBuildHistogram.UNDECIDED_CHILD_NODE_ID);
          if (nid < 0) continue; //skip OOB and otherwise skipped rows
          float f = (float)(preds.atd(row) + offset.atd(row));
          int idx = nid - _firstLeafIndex;
          _mins[idx] = Math.min(_mins[idx], f);
          _maxs[idx] = Math.max(_maxs[idx], f);
        }
      }

      @Override
      public void reduce(ComputeMinMax mrt) {
        ArrayUtils.reduceMin(_mins, mrt._mins);
        ArrayUtils.reduceMax(_maxs, mrt._maxs);
      }
    }

    final static private double MIN_LOG_TRUNC = -19;
    final static private double MAX_LOG_TRUNC = 19;

    private void truncatePreds(final DTree tree, int firstLeafIndex, DistributionFamily dist) {
      if (firstLeafIndex==tree._len) return;
      ComputeMinMax minMax = new ComputeMinMax(firstLeafIndex, tree._len).doAll(_train);
      if (DEV_DEBUG) {
        Log.info("Number of leaf nodes: " + minMax._mins.length);
        Log.info("Min: " + java.util.Arrays.toString(minMax._mins));
        Log.info("Max: " + java.util.Arrays.toString(minMax._maxs));
      }
      //loop over leaf nodes only: starting at leaf index
      for (int i = 0; i < tree._len - firstLeafIndex; i++) {
        final LeafNode node = ((LeafNode) tree.node(firstLeafIndex + i));
        int nidx = node.nid();
        float nodeMin = minMax._mins[nidx- firstLeafIndex];
        float nodeMax = minMax._maxs[nidx- firstLeafIndex];
//        Log.info("Node: " + nidx + " min/max: " + nodeMin + "/" + nodeMax);

        // https://github.com/cran/gbm/blob/master/src/poisson.cpp
        // https://github.com/harrysouthworth/gbm/blob/master/src/poisson.cpp
        // https://github.com/gbm-developers/gbm/blob/master/src/poisson.cpp

        // https://github.com/harrysouthworth/gbm/blob/master/src/gamma.cpp
        // https://github.com/gbm-developers/gbm/blob/master/src/gamma.cpp

        // https://github.com/harrysouthworth/gbm/blob/master/src/tweedie.cpp
        // https://github.com/gbm-developers/gbm/blob/master/src/tweedie.cpp
        double val = node._pred;
        if (dist == DistributionFamily.gamma || dist == DistributionFamily.tweedie) //only for gamma/tweedie
          val += nodeMax;
        if (val > MAX_LOG_TRUNC) {
//          Log.warn("Truncating large positive leaf prediction (log): " + node._pred + " to " + (MAX_LOG_TRUNC - nodeMax));
          node._pred = (float) (MAX_LOG_TRUNC - nodeMax);
        }
        val = node._pred;
        if (dist == DistributionFamily.gamma || dist == DistributionFamily.tweedie) //only for gamma/tweedie
          val += nodeMin;
        if (val < MIN_LOG_TRUNC) {
//          Log.warn("Truncating large negative leaf prediction (log): " + node._pred + " to " + (MIN_LOG_TRUNC - nodeMin));
          node._pred = (float) (MIN_LOG_TRUNC - nodeMin);
        }
        if (node._pred < MIN_LOG_TRUNC && node._pred > MAX_LOG_TRUNC) {
          Log.warn("Terminal node prediction outside of allowed interval in log-space: "
                  + node._pred + " (should be in " + MIN_LOG_TRUNC + "..." + MAX_LOG_TRUNC + ").");
        }
      }
    }
    ScoreBuildHistogram2.SCBParms _scbParms;

    // --------------------------------------------------------------------------
    // Build the next k-trees, which is trying to correct the residual error from
    // the prior trees.
    @Override protected boolean buildNextKTrees() {
      // We're going to build K (nclass) trees - each focused on correcting
      // errors for a single class.
      final DTree[] ktrees = new DTree[_nclass];

      // Define a "working set" of leaf splits, from here to tree._len
      int[] leaves = new int[_nclass];

      // Compute predictions and resulting residuals
      // ESL2, page 387, Steps 2a, 2b
      // fills "Work" columns for all rows (incl. OOB) with the residuals
      double huberDelta = Double.NaN;
      if (_parms._distribution == DistributionFamily.huber) {
        // Jerome Friedman 1999: Greedy Function Approximation: A Gradient Boosting Machine
        // https://statweb.stanford.edu/~jhf/ftp/trebst.pdf
        // compute absolute diff |y-(f+o)| for all rows
        Vec diff = new ComputeAbsDiff().doAll(1, (byte)3 /*numeric*/, _train).outputFrame().anyVec();
        Distribution dist = new Distribution(_parms);
        // compute weighted alpha-quantile of the absolute residual -> this is the delta for the huber loss
        huberDelta = MathUtils.computeWeightedQuantile(_weights, diff, _parms._huber_alpha);
        dist.setHuberDelta(huberDelta);
        // now compute residuals using the gradient of the huber loss (with a globally adjusted delta)
        new StoreResiduals(dist).doAll(_train, _parms._build_tree_one_node);
      } else {
        // compute predictions and residuals in one shot
        new ComputePredAndRes().doAll(_train, _parms._build_tree_one_node);
      }
      for (int k = 0; k < _nclass; k++) {
        if (DEV_DEBUG && ktrees[k]!=null) {
          System.out.println("Updated predictions in WORK col for class " + k + ":\n" + new Frame(new String[]{"WORK"},new Vec[]{vec_work(_train, k)}).toString());
        }
      }

      // ----
      // ESL2, page 387.  Step 2b ii.
      // One Big Loop till the ktrees are of proper depth.
      // Adds a layer to the trees each pass.
      growTrees(ktrees, leaves, _rand);
      for (int k = 0; k < _nclass; k++) {
        if (DEV_DEBUG && ktrees[k]!=null) {
          System.out.println("Grew trees. Updated NIDs for class " + k + ":\n" + new Frame(new String[]{"NIDS"},new Vec[]{vec_nids(_train, k)}).toString());
        }
      }

      // ----
      // ESL2, page 387.  Step 2b iii.  Compute the gammas (leaf node predictions === fit best constant), and store them back
      // into the tree leaves.  Includes learn_rate.
      GammaPass gp = new GammaPass(ktrees, leaves, new Distribution(_parms));
      gp.doAll(_train);
      if (_parms._distribution == DistributionFamily.laplace) {
        fitBestConstantsQuantile(ktrees, leaves[0], 0.5); //special case for Laplace: compute the median for each leaf node and store that as prediction
      } else if (_parms._distribution == DistributionFamily.quantile) {
        fitBestConstantsQuantile(ktrees, leaves[0], _parms._quantile_alpha); //compute the alpha-quantile for each leaf node and store that as prediction
      } else if (_parms._distribution == DistributionFamily.huber) {
        fitBestConstantsHuber(ktrees, leaves[0], huberDelta); //compute the alpha-quantile for each leaf node and store that as prediction
      } else {
        fitBestConstants(ktrees, leaves, gp);
      }

      // Apply a correction for strong mispredictions (otherwise deviance can explode)
      if (_parms._distribution == DistributionFamily.gamma ||
          _parms._distribution == DistributionFamily.poisson ||
          _parms._distribution == DistributionFamily.tweedie) {
        assert(_nclass == 1);
        truncatePreds(ktrees[0], leaves[0], _parms._distribution);
      }

      // ----
      // ESL2, page 387.  Step 2b iv.  Cache the sum of all the trees, plus the
      // new tree, in the 'tree' columns.  Also, zap the NIDs for next pass.
      // Tree <== f(Tree)
      // Nids <== 0
      new AddTreeContributions(ktrees).doAll(_train);

      // sanity check
      for (int k = 0; k < _nclass; k++) {
        if (ktrees[k]!=null) assert(vec_nids(_train,k).mean()==0);
      }

      // Grow the model by K-trees
      _model._output.addKTrees(ktrees);

      boolean converged = effective_learning_rate() < 1e-6;
      if (converged) {
        Log.warn("Effective learning rate dropped below 1e-6 (" + _parms._learn_rate + " * " + _parms._learn_rate_annealing + "^" + (_model._output._ntrees-1) + ") - stopping the model now.");
      }
      return converged;
    }

    private void growTrees(DTree[] ktrees, int[] leaves, Random rand) {
      // Initial set of histograms.  All trees; one leaf per tree (the root
      // leaf); all columns
      DHistogram hcs[][][] = new DHistogram[_nclass][1/*just root leaf*/][_ncols];

      // Adjust real bins for the top-levels
      int adj_nbins = Math.max(_parms._nbins_top_level,_parms._nbins);

      long rseed = rand.nextLong();
      // initialize trees
      for (int k = 0; k < _nclass; k++) {
        // Initially setup as-if an empty-split had just happened
        if (_model._output._distribution[k] != 0) {
          if (k == 1 && _nclass == 2) continue; // Boolean Optimization (only one tree needed for 2-class problems)
          ktrees[k] = new DTree(_train, _ncols, (char)_nclass, _mtry, _mtry_per_tree, rseed, _parms);
          DHistogram[] hist = DHistogram.initialHist(_train, _ncols, adj_nbins, hcs[k][0], rseed, _parms, getGlobalQuantilesKeys());
          new UndecidedNode(ktrees[k], DTree.NO_PARENT, hist); // The "root" node
        }
      }

      // Sample - mark the lines by putting 'OUT_OF_BAG' into nid(<klass>) vector
      if (_parms._sample_rate < 1 || _parms._sample_rate_per_class != null) {
        Sample ss[] = new Sample[_nclass];
        for (int k = 0; k < _nclass; k++)
          if (ktrees[k] != null)
            ss[k] = new Sample(ktrees[k], _parms._sample_rate, _parms._sample_rate_per_class).dfork(null, new Frame(vec_nids(_train, k), _response), _parms._build_tree_one_node);
        for (int k = 0; k < _nclass; k++) {
          if (ss[k] != null) {
            ss[k].getResult();
            if (DEV_DEBUG && ktrees[k]!=null) {
              System.out.println("Sampled OOB rows. NIDS:\n" + new Frame(vec_nids(_train, k)).toString());
            }
          }
        }
      }

      // ----
      // ESL2, page 387.  Step 2b ii.
      // One Big Loop till the ktrees are of proper depth.
      // Adds a layer to the trees each pass.
      int depth = 0;
      for (; depth < _parms._max_depth; depth++) {
        hcs = buildLayer(_train, _parms._nbins, _parms._nbins_cats, ktrees, leaves, hcs, _parms._build_tree_one_node,_scbParms);
        // If we did not make any new splits, then the tree is split-to-death
        if (hcs == null) break;
      }

      // Each tree bottomed-out in a DecidedNode; go 1 more level and insert
      // LeafNodes to hold predictions.
      for (int k = 0; k < _nclass; k++) {
        DTree tree = ktrees[k];
        if (tree == null) continue;
        int leaf = tree.len();
        leaves[k] = leaf; //record the size of the tree before splitting the bottom nodes as the starting index for the leaf node indices
        for (int nid = 0; nid < leaf; nid++) {
          if (tree.node(nid) instanceof DecidedNode) {
            DecidedNode dn = tree.decided(nid);
            if (dn._split == null) { // No decision here, no row should have this NID now
              if (nid == 0)               // Handle the trivial non-splitting tree
                new LeafNode(tree, DTree.NO_PARENT, 0);
              continue;
            }
            for (int i = 0; i < dn._nids.length; i++) { //L/R children
              int cnid = dn._nids[i];
              if (cnid == ScoreBuildHistogram.UNDECIDED_CHILD_NODE_ID ||    // Bottomed out (predictors or responses known constant)
                      tree.node(cnid) instanceof UndecidedNode || // Or chopped off for depth
                      (tree.node(cnid) instanceof DecidedNode &&  // Or not possible to split
                              ((DecidedNode) tree.node(cnid))._split == null))
                dn._nids[i] = new LeafNode(tree, nid).nid(); // Mark a leaf here
            }
          }
        }
      } // -- k-trees are done
    }

    private class ComputeDiff extends MRTask<ComputeDiff> {
      @Override
      public void map(Chunk[] chks, NewChunk[] nc) {
        final Chunk y = chk_resp(chks);
        final Chunk o = hasOffsetCol() ? chk_offset(chks) : new C0DChunk(0, chks[0]._len);
        final Chunk f = chk_tree(chks,0);
        for (int i=0; i<chks[0].len(); ++i)
          nc[0].addNum(y.atd(i) - (f.atd(i) + o.atd(i)));
      }
    }

    private class ComputeAbsDiff extends MRTask<ComputeAbsDiff> {
      @Override
      public void map(Chunk[] chks, NewChunk[] nc) {
        final Chunk y = chk_resp(chks);
        final Chunk o = hasOffsetCol() ? chk_offset(chks) : new C0DChunk(0, chks[0]._len);
        final Chunk f = chk_tree(chks,0);
        for (int i=0; i<chks[0].len(); ++i)
          nc[0].addNum(Math.abs(y.atd(i) - (f.atd(i) + o.atd(i))));
      }
    }

    private class StoreResiduals extends MRTask<StoreResiduals> {
      Distribution _dist;
      StoreResiduals(Distribution dist) { _dist = dist; }
      @Override public void map( Chunk chks[] ) {
        Chunk ys = chk_resp(chks);
        Chunk offset = hasOffsetCol() ? chk_offset(chks) : new C0DChunk(0, chks[0]._len);
        Chunk preds = chk_tree(chks, 0); // Prior tree sums
        Chunk wk = chk_work(chks, 0); // Place to store residuals
        Chunk weights = hasWeightCol() ? chk_weight(chks) : new C0DChunk(1, chks[0]._len);
        for( int row = 0; row < wk._len; row++) {
          double weight = weights.atd(row);
          if (weight == 0) continue;
          if (ys.isNA(row)) continue;
          double f = preds.atd(row) + offset.atd(row);
          double y = ys.atd(row);
          wk.set(row, (float) _dist.negHalfGradient(y, f));
        }
      }
    }

    private void fitBestConstantsQuantile(DTree[] ktrees, int firstLeafIndex, double quantile) {
      if (firstLeafIndex == ktrees[0]._len) return; // no splits happened - nothing to do
      assert(_nclass==1);
      Vec diff = new ComputeDiff().doAll(1, (byte)3 /*numeric*/, _train).outputFrame().anyVec();
      Vec weights = hasWeightCol() ? _train.vecs()[idx_weight()] : null;
      Vec strata = vec_nids(_train,0);

      // compute quantile for all leaf nodes
      Quantile.StratifiedQuantilesTask sqt = new Quantile.StratifiedQuantilesTask(null, quantile, diff, weights, strata, QuantileModel.CombineMethod.INTERPOLATE);
      H2O.submitTask(sqt);
      sqt.join();

      final DTree tree = ktrees[0];
      for (int i = 0; i < sqt._quantiles.length; i++) {
        if (Double.isNaN(sqt._quantiles[i])) continue; //no active rows for this NID
        double val = effective_learning_rate() * sqt._quantiles[i];
        assert !Double.isNaN(val) && !Double.isInfinite(val);
        if (val > _parms._max_abs_leafnode_pred) val = _parms._max_abs_leafnode_pred;
        if (val < -_parms._max_abs_leafnode_pred) val = -_parms._max_abs_leafnode_pred;
        ((LeafNode) tree.node(sqt._nids[i]))._pred = (float) val;
        if (DEV_DEBUG) { Log.info("Leaf " + sqt._nids[i] + " has quantile: " + sqt._quantiles[i]); }
      }
    }

    public class DiffMinusMedianDiff extends MRTask<DiffMinusMedianDiff> {
      Vec _strata;
      double[] _terminalMedians;
      DiffMinusMedianDiff(Vec strata, double[] terminalMedians) {
        _strata = strata;
        _terminalMedians = terminalMedians;
      }
      @Override
      public void map(Chunk[] chks) {
        final Chunk strata = chks[0];
        final Chunk diff = chks[1];
        final int strataMin = (int)_strata.min();
        for (int i=0; i<chks[0].len(); ++i) {
          int nid = (int)strata.atd(i);
          diff.set(i, diff.atd(i) - _terminalMedians[nid-strataMin]);
        }
      }
    }

    private final class HuberLeafMath extends MRTask<HuberLeafMath> {
      // INPUT
      final double _huberDelta;
      final Vec _strata;
      // OUTPUT
      double[/*leaves*/] _huberGamma, _wcounts;
      public HuberLeafMath(double huberDelta, Vec strata) {
        _huberDelta = huberDelta;
        _strata = strata;
      }
      @Override
      public void map(Chunk cs[]) {
        final int strataMin = (int)_strata.min();
        final int strataMax = (int)_strata.max();
        if (strataMin < 0 || strataMax < 0) {
          Log.warn("No Huber math can be done since there's no strata.");
          return;
        }
        final int nstrata = strataMax - strataMin + 1;
        Log.info("Computing Huber math for (up to) " + nstrata + " different strata.");
        _huberGamma = new double[nstrata];
        _wcounts = new double[nstrata];
        Chunk weights = hasWeightCol() ? chk_weight(cs) : new C0DChunk(1, cs[0]._len);
        Chunk stratum = chk_nids(cs, 0 /*regression*/);
        Chunk diffMinusMedianDiff = cs[cs.length-1];
        for (int row=0;row<cs[0]._len;++row) {
          int nidx = (int) stratum.at8(row) - strataMin; //get terminal node for this row
          _huberGamma[nidx] += weights.atd(row) * Math.signum(diffMinusMedianDiff.atd(row)) * Math.min(Math.abs(diffMinusMedianDiff.atd(row)), _huberDelta);
                  _wcounts[nidx] += weights.atd(row);
        }
      }
      @Override
      public void reduce(HuberLeafMath mrt) {
        ArrayUtils.add(_huberGamma,mrt._huberGamma);
        ArrayUtils.add(_wcounts,mrt._wcounts);
      }

      @Override
      protected void postGlobal() {
        for (int i = 0; i< _huberGamma.length; ++i)
          _huberGamma[i]/=_wcounts[i];
      }
    }

    // Jerome Friedman 1999: Greedy Function Approximation: A Gradient Boosting Machine
    // https://statweb.stanford.edu/~jhf/ftp/trebst.pdf
    private void fitBestConstantsHuber(DTree[] ktrees, int firstLeafIndex, double huberDelta) {
      if (firstLeafIndex == ktrees[0]._len) return; // no splits happened - nothing to do
      assert(_nclass==1);

      // get diff y-(f+o) and weights and strata (node idx)
      Vec diff = new ComputeDiff().doAll(1, (byte)3 /*numeric*/, _train).outputFrame().anyVec();
      Vec weights = hasWeightCol() ? _train.vecs()[idx_weight()] : null;
      Vec strata = vec_nids(_train,0);

      // compute median diff for each leaf node
      Quantile.StratifiedQuantilesTask sqt = new Quantile.StratifiedQuantilesTask(null, 0.5 /*median of weighted residuals*/, diff, weights, strata, QuantileModel.CombineMethod.INTERPOLATE);
      H2O.submitTask(sqt);
      sqt.join();

      // subtract median(diff) from residuals for all observations of each leaf
      DiffMinusMedianDiff hp = new DiffMinusMedianDiff(strata, sqt._quantiles /*median residuals per leaf*/);
      Frame tmpFrame1 = new Frame(new String[]{"strata","diff"}, new Vec[]{strata,diff});
      hp.doAll(tmpFrame1);
      Vec diffMinusMedianDiff = diff;

      // for each leaf, compute the mean of Math.signum(resMinusMedianRes) * Math.min(Math.abs(resMinusMedianRes), huberDelta),
      // where huberDelta is the alpha-percentile of the residual across all observations
      Frame tmpFrame2 = new Frame(_train.vecs());
      tmpFrame2.add("resMinusMedianRes", diffMinusMedianDiff);
      double[] huberGamma = new HuberLeafMath(huberDelta,strata).doAll(tmpFrame2)._huberGamma;

      // now assign the median per leaf + the above _huberCorrection[i] to each leaf
      final DTree tree = ktrees[0];
      for (int i = 0; i < sqt._quantiles.length; i++) {
        double huber = (sqt._quantiles[i] /*median*/ + huberGamma[i]);
        if (Double.isNaN(sqt._quantiles[i])) continue; //no active rows for this NID
        double val = effective_learning_rate() * huber;
        assert !Double.isNaN(val) && !Double.isInfinite(val);
        if (val > _parms._max_abs_leafnode_pred) val = _parms._max_abs_leafnode_pred;
        if (val < -_parms._max_abs_leafnode_pred) val = -_parms._max_abs_leafnode_pred;
        ((LeafNode) tree.node(sqt._nids[i]))._pred = (float) val;
        if (DEV_DEBUG) { Log.info("Leaf " + sqt._nids[i] + " has huber value: " + huber); }
      }
      diffMinusMedianDiff.remove();
    }

    private double effective_learning_rate() {
      return _parms._learn_rate * Math.pow(_parms._learn_rate_annealing, (_model._output._ntrees-1));
    }

    private void fitBestConstants(DTree[] ktrees, int[] leafs, GammaPass gp) {
      double m1class = _nclass > 1 && _parms._distribution == DistributionFamily.multinomial ? (double) (_nclass - 1) / _nclass : 1.0; // K-1/K for multinomial
      for (int k = 0; k < _nclass; k++) {
        final DTree tree = ktrees[k];
        if (tree == null) continue;
        if (DEV_DEBUG) for (int i=0;i<ktrees[k]._len-leafs[k];++i) System.out.println(ktrees[k].node(leafs[k]+i).toString());
        for (int i = 0; i < tree._len - leafs[k]; i++) {
          double gf = effective_learning_rate() * m1class * gp.gamma(k, i);
          // In the multinomial case, check for very large values (which will get exponentiated later)
          // Note that gss can be *zero* while rss is non-zero - happens when some rows in the same
          // split are perfectly predicted true, and others perfectly predicted false.
          if (_parms._distribution == DistributionFamily.multinomial) {
            if (gf > 1e4) gf = 1e4f; // Cap prediction, will already overflow during Math.exp(gf)
            else if (gf < -1e4) gf = -1e4f;
          }
          if (Double.isNaN(gf)) gf=0;
          else if (Double.isInfinite(gf)) gf=Math.signum(gf)*1e4f;
          if (gf > _parms._max_abs_leafnode_pred) gf = _parms._max_abs_leafnode_pred;
          if (gf < -_parms._max_abs_leafnode_pred) gf = -_parms._max_abs_leafnode_pred;
          ((LeafNode) tree.node(leafs[k] + i))._pred = (float) gf;
        }
      }
    }

    // Set terminal node estimates (gamma)
    // ESL2, page 387.  Step 2b iii.
    // Nids <== f(Nids)
    // For classification (bernoulli):
    //    gamma_i = sum (w_i * res_i) / sum (w_i*p_i*(1 - p_i)) where p_i = y_i - res_i
    // For classification (multinomial):
    //    gamma_i_k = (nclass-1)/nclass * (sum res_i / sum (|res_i|*(1-|res_i|)))
    // For regression (gaussian):
    //    gamma_i = sum res_i / count(res_i)
    private class GammaPass extends MRTask<GammaPass> {
      final DTree _trees[]; // Read-only, shared (except at the histograms in the Nodes)
      final int _leafs[];  // Starting index of leaves (per class-tree)
      final Distribution _dist;
      private double _num[/*tree/klass*/][/*tree-relative node-id*/];
      private double _denom[/*tree/klass*/][/*tree-relative node-id*/];

      double gamma(int tree, int nid) {
        if (_denom[tree][nid] == 0) return 0;
        double g = _num[tree][nid]/ _denom[tree][nid];
        assert (!Double.isInfinite(g) && !Double.isNaN(g));
        if (_dist.distribution == DistributionFamily.poisson ||
            _dist.distribution == DistributionFamily.gamma ||
            _dist.distribution == DistributionFamily.tweedie)
        {
          return _dist.link(g);
        } else {
          return g;
        }
      }

      GammaPass(DTree trees[],
                int leafs[],
                Distribution distribution
      ) {
        _leafs=leafs;
        _trees=trees;
        _dist = distribution;
      }
      @Override public void map( Chunk[] chks ) {
        _denom = new double[_nclass][];
        _num = new double[_nclass][];
        final Chunk resp = chk_resp(chks); // Response for this frame

        // For all tree/klasses
        for( int k=0; k<_nclass; k++ ) {
          final DTree tree = _trees[k];
          final int   leaf = _leafs[k];
          if( tree == null ) continue; // Empty class is ignored
          assert(tree._len-leaf >= 0);

          // A leaf-biased array of all active Tree leaves.
          final double denom[] = _denom[k] = new double[tree._len-leaf];
          final double num[] = _num[k] = new double[tree._len-leaf];
          final Chunk nids = chk_nids(chks, k); // Node-ids  for this tree/class
          final Chunk ress = chk_work(chks, k); // Residuals for this tree/class
          final Chunk offset = hasOffsetCol() ? chk_offset(chks) : new C0DChunk(0, chks[0]._len); // Residuals for this tree/class
          final Chunk preds = chk_tree(chks,k);
          final Chunk weights = hasWeightCol() ? chk_weight(chks) : new C0DChunk(1, chks[0]._len);

          // If we have all constant responses, then we do not split even the
          // root and the residuals should be zero.
          if( tree.root() instanceof LeafNode )
            continue;
          Distribution dist = new Distribution(_parms);
          for( int row=0; row<nids._len; row++ ) { // For all rows
            double w = weights.atd(row);
            if (w==0) continue;

            double y = resp.atd(row); //response
            if (Double.isNaN(y)) continue;

            // Compute numerator and denominator of terminal node estimate (gamma)
            int nid = (int)nids.at8(row);          // Get Node to decide from
            final boolean wasOOBRow = ScoreBuildHistogram.isOOBRow(nid); //same for all k
            if (wasOOBRow) nid = ScoreBuildHistogram.oob2Nid(nid);
            if (nid < 0) continue;
            if( tree.node(nid) instanceof UndecidedNode ) // If we bottomed out the tree
              nid = tree.node(nid).pid();                  // Then take parent's decision
            DecidedNode dn = tree.decided(nid);           // Must have a decision point
            if( dn._split == null )                    // Unable to decide?
              dn = tree.decided(dn.pid());  // Then take parent's decision
            int leafnid = dn.getChildNodeID(chks[dn._split._col].atd(row)); // Decide down to a leafnode
            assert leaf <= leafnid && leafnid < tree._len :
                    "leaf: " + leaf + " leafnid: " + leafnid + " tree._len: " + tree._len + "\ndn: " + dn;
            assert tree.node(leafnid) instanceof LeafNode;
            // Note: I can tell which leaf/region I end up in, but I do not care for
            // the prediction presented by the tree.  For GBM, we compute the
            // sum-of-residuals (and sum/abs/mult residuals) for all rows in the
            // leaf, and get our prediction from that.
            nids.set(row, leafnid);
            assert !ress.isNA(row);

            // OOB rows get placed properly (above), but they don't affect the computed Gamma (below)
            // For Laplace/Quantile distribution, we need to compute the median of (y-offset-preds == y-f), will be done outside of here
            if (wasOOBRow
                    || _parms._distribution == DistributionFamily.laplace
                    || _parms._distribution == DistributionFamily.huber
                    || _parms._distribution == DistributionFamily.quantile) continue;

            double z = ress.atd(row); //residual
            double f = preds.atd(row) + offset.atd(row);
            int idx=leafnid-leaf;
            num[idx] += dist.gammaNum(w, y, z, f);
            denom[idx] += dist.gammaDenom(w, y, z, f);
          }
        }
      }
      @Override public void reduce( GammaPass gp ) {
        ArrayUtils.add(_denom,gp._denom);
        ArrayUtils.add(_num,gp._num);
      }
    }

    private class AddTreeContributions extends MRTask<AddTreeContributions> {
      DTree[] _ktrees;
      AddTreeContributions(DTree[] ktrees) { _ktrees = ktrees; }
      @Override public void map( Chunk chks[] ) {
        // For all tree/klasses
        for( int k=0; k<_nclass; k++ ) {
          final DTree tree = _ktrees[k];
          if( tree == null ) continue;
          final Chunk nids = chk_nids(chks,k);
          final Chunk ct   = chk_tree(chks, k);
          final Chunk y   = chk_resp(chks);
          final Chunk weights = hasWeightCol() ? chk_weight(chks) : new C0DChunk(1, chks[0]._len);
          long baseseed = (0xDECAF + _parms._seed) * (0xFAAAAAAB + k * _parms._ntrees + _model._output._ntrees);
          for( int row=0; row<nids._len; row++ ) {
            int nid = (int)nids.at8(row);
            nids.set(row, ScoreBuildHistogram.FRESH);
            if( nid < 0 ) continue;
            if (y.isNA(row)) continue;
            if (weights.atd(row)==0) continue;
            double factor = 1;
            if (_parms._pred_noise_bandwidth !=0) {
              _rand.setSeed(baseseed + nid); //bandwidth is a function of tree number, class and node id (but same for all rows in that node)
              factor += _rand.nextGaussian() * _parms._pred_noise_bandwidth;
            }
            // Prediction stored in Leaf is cut to float to be deterministic in reconstructing
            // <tree_klazz> fields from tree prediction
            ct.set(row, (float)(ct.atd(row) + factor * ((LeafNode)tree.node(nid))._pred ));
          }
        }
      }
    }

    @Override protected GBMModel makeModel( Key modelKey, GBMModel.GBMParameters parms) {
      return new GBMModel(modelKey,parms,new GBMModel.GBMOutput(GBM.this));
    }

  }

  // Read the 'tree' columns, do model-specific math and put the results in the
  // fs[] array, and return the sum.  Dividing any fs[] element by the sum
  // turns the results into a probability distribution.
  @Override protected double score1( Chunk chks[], double weight, double offset, double fs[/*nclass*/], int row ) {
    double f = chk_tree(chks,0).atd(row) + offset;
    double p = new Distribution(_parms).linkInv(f);
    if( _parms._distribution == DistributionFamily.modified_huber || _parms._distribution == DistributionFamily.bernoulli ) {
      fs[2] = p;
      fs[1] = 1.0-p;
      return 1;                 // f2 = 1.0 - f1; so f1+f2 = 1.0
    } else if (_parms._distribution == DistributionFamily.multinomial) {
      if (_nclass == 2) {
        // This optimization assumes the 2nd tree of a 2-class system is the
        // inverse of the first.  Fill in the missing tree
        fs[1] = p;
        fs[2] = 1 / p;
        return fs[1] + fs[2];
      }
      // Multinomial loss function; sum(exp(data)).  Load tree data
      assert(offset==0);
      fs[1] = f;
      for( int k=1; k<_nclass; k++ )
        fs[k+1]=chk_tree(chks,k).atd(row);
      // Rescale to avoid Infinities; return sum(exp(data))
      return hex.genmodel.GenModel.log_rescale(fs);
    }
    else {
      return fs[0] = p;
    }
  }
}
